package client

import (
	"errors"
	"fmt"
)

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

// NotLeaderError is a typed ERR NOTLEADER <hint> from a replicated broker: a
// leader-gated op (write, or a default SUB) reached a follower. Hint is the
// raft node id of the believed leader ("" if unknown). ClusterClient matches
// it with errors.As to resolve the hint and redirect; a bare Client surfaces
// it so a caller can too (v3 M3, ADR 0032).
type NotLeaderError struct {
	Hint string
}

func (e *NotLeaderError) Error() string {
	if e.Hint == "" {
		return "client: not leader (no leader hint)"
	}
	return fmt.Sprintf("client: not leader, redirect to %q", e.Hint)
}

// serverErr converts a frameErr into the appropriate typed/sentinel error,
// shared by every request path (PUB/SUB/ACK/NACK/CREATE/flow) so NOTLEADER
// and TRANSPORT are classified identically everywhere.
func serverErr(f frame) error {
	switch f.errCode {
	case "TRANSPORT":
		return fmt.Errorf("%w: %s", ErrTransport, f.errMsg)
	case "NOTLEADER":
		return &NotLeaderError{Hint: f.errMsg}
	default:
		return fmt.Errorf("%w: %s %s", ErrServer, f.errCode, f.errMsg)
	}
}
