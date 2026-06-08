package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"time"
)

type Config struct {
	Addr            string
	DataDir         string
	LogLevel        string
	LogFormat       string
	ShutdownTimeout time.Duration
	DedupeCap       int
}

const (
	DefaultAddr            = ":6789"
	DefaultDataDir         = "./data"
	DefaultLogLevel        = "info"
	DefaultLogFormat       = "text"
	DefaultShutdownTimeout = 5 * time.Second
	DefaultDedupeCap       = 4096
)

var (
	validLogLevels  = map[string]struct{}{"debug": {}, "info": {}, "warn": {}, "error": {}}
	validLogFormats = map[string]struct{}{"text": {}, "json": {}}
)

// Parse reads args (typically os.Args[1:]) and produces a validated
// Config. Output destined for the user (e.g. -h help text) goes to
// stderr. Errors are returned, not printed.
func Parse(args []string, stderr io.Writer) (*Config, error) {
	fs := flag.NewFlagSet("toymq", flag.ContinueOnError)
	fs.SetOutput(stderr)

	cfg := &Config{}
	fs.StringVar(&cfg.Addr, "addr", DefaultAddr, "TCP listen address")
	fs.StringVar(&cfg.DataDir, "data-dir", DefaultDataDir, "broker storage root")
	fs.StringVar(&cfg.LogLevel, "log-level", DefaultLogLevel, "debug|info|warn|error")
	fs.StringVar(&cfg.LogFormat, "log-format", DefaultLogFormat, "text|json")
	fs.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", DefaultShutdownTimeout, "graceful drain budget")
	fs.IntVar(&cfg.DedupeCap, "dedupe-cap", DefaultDedupeCap, "per-topic dedupe LRU size")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if c.Addr == "" {
		return errors.New("addr must not be empty")
	}
	if c.DataDir == "" {
		return errors.New("data-dir must not be empty")
	}
	if _, ok := validLogLevels[c.LogLevel]; !ok {
		return fmt.Errorf("log-level %q: must be debug|info|warn|error", c.LogLevel)
	}
	if _, ok := validLogFormats[c.LogFormat]; !ok {
		return fmt.Errorf("log-format %q: must be text|json", c.LogFormat)
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("shutdown-timeout %v: must be > 0", c.ShutdownTimeout)
	}
	if c.DedupeCap <= 0 {
		return fmt.Errorf("dedupe-cap %d: must be > 0", c.DedupeCap)
	}
	return nil
}
