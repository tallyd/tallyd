// Command tallyd is a self-hosted, vendor-agnostic daemon that buffers
// local usage events durably and forwards them to billing providers.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/earthy1024/tallyd/internal/pipeline"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "tallyd:", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to YAML config file (defaults to built-in defaults if omitted)")
	listenAddr := flag.String("listen", "", "override the HTTP listen address from config (host:port)")
	grpcListenAddr := flag.String("grpc-listen", "", "override the gRPC listen address from config (host:port); gRPC stays disabled unless this or config's listen.grpc is set")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		return err
	}
	if *listenAddr != "" {
		cfg.Listen.HTTP = *listenAddr
	}
	if *grpcListenAddr != "" {
		cfg.Listen.GRPC = *grpcListenAddr
	}

	p, err := pipeline.Build(cfg)
	if err != nil {
		return fmt.Errorf("build pipeline: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.RunGauges(ctx, 2*time.Second)

	httpLis, err := net.Listen("tcp", cfg.Listen.HTTP)
	if err != nil {
		return fmt.Errorf("http listen on %s: %w", cfg.Listen.HTTP, err)
	}
	server := &http.Server{Handler: p.Handler()}

	// Buffered for 2: both the HTTP and (if enabled) gRPC goroutines can
	// send here, but only the first is ever read by the select below. A
	// graceful stop doesn't trigger a second send (grpc.Server.Serve
	// returns nil, not an error, when GracefulStop causes it to return),
	// but a genuine error from either server could still coincide with
	// the other's send — size 1 would leave that sender blocked forever.
	serverErrCh := make(chan error, 2)
	go func() {
		// Log the listener's actual bound address, not cfg.Listen.HTTP —
		// they differ whenever the configured port is 0 (OS-assigned),
		// and printing the pre-resolution string would be misleading.
		log.Printf("tallyd HTTP listening on %s", httpLis.Addr())
		if err := server.Serve(httpLis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
			return
		}
		serverErrCh <- nil
	}()

	if p.GRPCServer != nil {
		grpcLis, err := net.Listen("tcp", cfg.Listen.GRPC)
		if err != nil {
			return fmt.Errorf("grpc listen on %s: %w", cfg.Listen.GRPC, err)
		}
		go func() {
			log.Printf("tallyd gRPC listening on %s", cfg.Listen.GRPC)
			if err := p.GRPCServer.Serve(grpcLis); err != nil {
				serverErrCh <- err
			}
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErrCh:
		if err != nil {
			return fmt.Errorf("server error: %w", err)
		}
	case sig := <-sigCh:
		log.Printf("received %s, shutting down", sig)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("http server shutdown: %v", err)
	}
	if p.GRPCServer != nil {
		p.GRPCServer.GracefulStop()
	}

	cancel() // stop gauge refresh loop

	if err := p.Close(); err != nil {
		return fmt.Errorf("pipeline close: %w", err)
	}
	return nil
}

// loadConfig reads path if given, otherwise returns a Config with only
// built-in defaults applied (useful for local smoke-testing without a
// YAML file on disk).
func loadConfig(path string) (*pipeline.Config, error) {
	if path == "" {
		cfg := &pipeline.Config{}
		return cfg, nil
	}
	return pipeline.LoadConfig(path)
}
