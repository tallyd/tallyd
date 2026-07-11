// Command tallyd is a self-hosted, vendor-agnostic daemon that buffers
// local usage events durably and forwards them to billing providers.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
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
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		return err
	}
	if *listenAddr != "" {
		cfg.Listen.HTTP = *listenAddr
	}

	p, err := pipeline.Build(cfg)
	if err != nil {
		return fmt.Errorf("build pipeline: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.RunGauges(ctx, 2*time.Second)

	server := &http.Server{Addr: cfg.Listen.HTTP, Handler: p.Handler()}

	serverErrCh := make(chan error, 1)
	go func() {
		log.Printf("tallyd listening on %s", cfg.Listen.HTTP)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
			return
		}
		serverErrCh <- nil
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErrCh:
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
	case sig := <-sigCh:
		log.Printf("received %s, shutting down", sig)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("http server shutdown: %v", err)
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
