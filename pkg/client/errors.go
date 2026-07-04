package client

import "errors"

// Sentinel errors. Wrap with %w when returning so callers can
// errors.Is against these.
var (
	// ErrClosed is returned by any method invoked after Close, or by
	// an in-flight call whose Client was closed under it.
	ErrClosed = errors.New("client: closed")

	// ErrTransport wraps any net.Conn read/write failure. After it
	// surfaces, the Client is closed and unusable.
	ErrTransport = errors.New("client: transport")

	// ErrServer wraps an ERR frame from the broker. Use errors.Is to
	// detect; the wrapped message carries the code and reason.
	ErrServer = errors.New("client: server error")

	// ErrSubInUse is returned by a second Sub on a Client that
	// already owns a subscription.
	ErrSubInUse = errors.New("client: subscription already active")

	// ErrHandshake is returned by Dial when the HELLO handshake fails
	// (bad version, malformed response, or a rejecting ERR). See ADR 0020.
	ErrHandshake = errors.New("client: handshake failed")

	// ErrAuth wraps ErrHandshake for the specific case of an AUTH
	// rejection, so callers can errors.Is against it to distinguish a
	// bad/missing token from other handshake failures.
	ErrAuth = errors.New("client: authentication failed")
)
