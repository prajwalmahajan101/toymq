package proto

import "errors"

var (
	ErrInvalidCommand  = errors.New("proto: invalid command")
	ErrPayloadTooLarge = errors.New("proto: payload too large")
	ErrShortBody       = errors.New("proto: short body")
	ErrBadFraming      = errors.New("proto: bad framing")
)

const MaxLineLength = 1 << 16 // 64kiB cap for header lines
