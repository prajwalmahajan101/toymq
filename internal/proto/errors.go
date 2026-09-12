package proto

import "errors"

var (
	ErrInvalidCommand  = errors.New("proto: invalid command")
	ErrPayloadTooLarge = errors.New("proto: payload too large")
	ErrShortBody       = errors.New("proto: short body")
	ErrBadFraming      = errors.New("proto: bad framing")
	ErrNotHello        = errors.New("proto: not a HELLO frame")
)

// Wire ERR codes introduced with the HELLO handshake (ADR 0020).
// Steady-state codes (INVALID, PUB_FAILED, ...) stay defined at their
// call sites; these two are shared between server and client.
const (
	ErrCodeHello = "HELLO" // missing/malformed handshake or unsupported version
	ErrCodeAuth  = "AUTH"  // missing or invalid AUTH token
	// ErrCodeNotLeader is returned for a write sent to a non-leader node in a
	// replicated cluster; the reason carries the leader's node id (v3 M2, ADR
	// 0030). Client-side redirect resolution + auto-retry is M3.
	ErrCodeNotLeader = "NOTLEADER"
	// ErrCodeWaitTimeout is returned when a PUB … WAIT <n> <ms> barrier is not
	// met within the timeout (v3 M4, ADR 0033). The write itself is
	// quorum-durable and committed — only the caller-requested replication
	// factor was not reached in time — so the reason carries the assigned
	// MsgID, not a failure.
	ErrCodeWaitTimeout = "WAIT_TIMEOUT"
)

const MaxLineLength = 1 << 16 // 64kiB cap for header lines
