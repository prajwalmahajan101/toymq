package client

import "context"

// Delivery is one server-pushed MSG, with closures to acknowledge it.
type Delivery struct {
	Topic   string
	MsgID   uint64
	Payload []byte
	Ack     func(context.Context) error
	Nack    func(context.Context) error
}
