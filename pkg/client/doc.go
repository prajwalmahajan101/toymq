// Package client is a reusable Go client for the ToyMQ wire protocol.
//
// A Client owns one TCP connection. Internally it runs a single read
// goroutine and serializes all writes through a mutex, mirroring the
// server's single-writer invariant. Pub blocks until the broker
// answers OK/DUP/ERR; Sub returns a channel of Delivery values whose
// Ack/Nack closures write the corresponding frames.
//
// Reconnect is the caller's responsibility: any transport failure
// permanently closes the Client and surfaces as ErrTransport. Wrap
// with your own backoff loop, or use a higher-level helper once one
// lands.
//
// Logging is opt-in. The Client is silent by default (see ADR 0013);
// pass WithLogger to attach a slog.Logger and receive debug records
// on Dial / Sub / Close and a warn on transport loss:
//
//	c, err := client.Dial(ctx, addr, client.WithLogger(slog.Default()))
package client
