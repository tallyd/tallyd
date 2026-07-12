package pipeline

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration wraps time.Duration to accept YAML strings like "10s" or "30m"
// (encoding/yaml doesn't do this for the bare stdlib type).
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	d.Duration = parsed
	return nil
}

// Config is tallyd's top-level configuration.
type Config struct {
	Listen    ListenConfig              `yaml:"listen"`
	Buffer    BufferConfig              `yaml:"buffer"`
	Providers map[string]ProviderConfig `yaml:"providers"`
	Routing   RoutingConfig             `yaml:"routing"`
}

type ListenConfig struct {
	HTTP string `yaml:"http"`
	// GRPC is optional; leave empty to disable the gRPC listener entirely
	// and run HTTP-only.
	GRPC string `yaml:"grpc"`
}

type BufferConfig struct {
	Dir string `yaml:"dir"`
	// MaxBytes caps the WAL's total on-disk size across all segments.
	// <= 0 (the zero value, i.e. unset) means unlimited. Once the cap is
	// hit, new Appends are rejected (wal.ErrBufferFull), which the
	// receiver surfaces to the caller as a 503/Unavailable — see
	// wal.WAL.TotalBytes and Append.
	MaxBytes int64 `yaml:"max_bytes"`
	// OnFull must be "reject" — the only implemented policy so far.
	// pipeline.Build fails fast at startup if it's set to anything else
	// (e.g. "drop_best_effort", which isn't built yet).
	OnFull string `yaml:"on_full"`
}

// ProviderConfig describes one billing provider target. Type selects
// which adapter.Adapter implementation to use; only "stdout" is
// implemented in this first pass (Orb and Metronome adapters are the
// next unit of work). Endpoint and TokenEnv are carried through now so
// the config format doesn't need to change once real adapters land.
type ProviderConfig struct {
	Type     string      `yaml:"type"`
	Endpoint string      `yaml:"endpoint"`
	TokenEnv string      `yaml:"token_env"`
	Batch    BatchConfig `yaml:"batch"`
	Retry    RetryConfig `yaml:"retry"`
}

type BatchConfig struct {
	MaxEvents int      `yaml:"max_events"`
	Linger    Duration `yaml:"linger"`
}

type RetryConfig struct {
	MaxElapsed  Duration `yaml:"max_elapsed"`
	MaxInterval Duration `yaml:"max_interval"`
}

type RoutingConfig struct {
	Default []string      `yaml:"default"`
	Rules   []RoutingRule `yaml:"rules"`
}

type RoutingRule struct {
	Match RoutingMatch `yaml:"match"`
	Route []string     `yaml:"route"`
}

type RoutingMatch struct {
	EventName string `yaml:"event_name"`
}

// LoadConfig reads and parses a YAML config file, applying defaults for
// anything left unset.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	cfg.applyDefaults()
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Listen.HTTP == "" {
		c.Listen.HTTP = "127.0.0.1:8999"
	}
	if c.Buffer.Dir == "" {
		c.Buffer.Dir = "/var/lib/tallyd/wal"
	}
	if c.Buffer.OnFull == "" {
		c.Buffer.OnFull = "reject"
	}
	for name, pc := range c.Providers {
		if pc.Type == "" {
			pc.Type = "stdout"
		}
		if pc.Batch.MaxEvents <= 0 {
			pc.Batch.MaxEvents = 500
		}
		if pc.Batch.Linger.Duration <= 0 {
			pc.Batch.Linger.Duration = 10 * time.Second
		}
		if pc.Retry.MaxElapsed.Duration <= 0 {
			pc.Retry.MaxElapsed.Duration = 30 * time.Minute
		}
		if pc.Retry.MaxInterval.Duration <= 0 {
			pc.Retry.MaxInterval.Duration = 2 * time.Minute
		}
		c.Providers[name] = pc
	}
}
