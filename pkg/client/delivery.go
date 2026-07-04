package client

import "context"

// Delivery is one server-pushed MSG, with closures to acknowledge it.
// Partition identifies which partition the message came from; MsgIDs are
// partition-local, so the Ack/Nack closures carry it back (ADR 0021).
type Delivery struct {
	Topic     string
	Partition int
	MsgID     uint64
	Payload   []byte
	Ack       func(context.Context) error
	Nack      func(context.Context) error
}
