package client

import (
	"context"
	"crypto/tls"
	"log/slog"
)

// config holds the resolved Client configuration. Defaults are set
// in newConfig; Options mutate it.
type config struct {
	logger        *slog.Logger
	authToken     string
	tlsConfig     *tls.Config
	traceparentFn func(context.Context) (traceparent, tracestate string)
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

// WithAuth sends the given bearer token in the HELLO handshake
// (HELLO 1 AUTH <token>). Required when the broker enables auth; ignored
// by a broker without auth. See ADR 0020.
func WithAuth(token string) Option {
	return func(c *config) { c.authToken = token }
}

// WithTLS dials over TLS using cfg (e.g. a config whose RootCAs trust the
// broker's cert). When set, Dial connects to the broker's TLS listener.
// nil (the default) dials plaintext.
func WithTLS(cfg *tls.Config) Option {
	return func(c *config) { c.tlsConfig = cfg }
}

// WithTraceparentFunc enables opt-in W3C trace propagation (ADR 0026)
// without coupling pkg/client to OpenTelemetry (ADR 0013 keeps the client
// stdlib-only). Before each PUB/SUB the Client calls fn with the call's
// context; when it returns a non-empty traceparent, the Client prepends a
// "TRACEPARENT <traceparent> [TRACESTATE <tracestate>]" line so the broker
// span becomes a child of the caller's span. OTel users wire
// tracing.TraceparentFromContext here; the default (nil) sends no trace
// line and behaves exactly as pre-M7.
func WithTraceparentFunc(fn func(context.Context) (traceparent, tracestate string)) Option {
	return func(c *config) { c.traceparentFn = fn }
}
