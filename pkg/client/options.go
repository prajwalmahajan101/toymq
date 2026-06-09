package client

import "log/slog"

// config holds the resolved Client configuration. Defaults are set
// in newConfig; Options mutate it.
type config struct {
	logger *slog.Logger
}

func newConfig() config {
	return config{}
}

// Option mutates a Client's configuration during Dial. New options
// can be added without breaking the Dial signature.
type Option func(*config)

// WithLogger attaches a slog.Logger so the Client emits state-change
// records on Dial, Sub, Close, and transport loss. Without this
// Option the Client is silent (the v1 default — see ADR 0013), so
// adding a logger is strictly opt-in.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) { c.logger = l }
}
