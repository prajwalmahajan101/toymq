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
)

const MaxLineLength = 1 << 16 // 64kiB cap for header lines
