package broker

import (
	"sync"
	"time"
)

type Inflight struct {
	MsgID       uint64
	Topic       string
	Payload     []byte
	DeliveredAt time.Time
	Attempts    int
}

type Consumer struct {
	ID    string
	topic *Topic

	mu        sync.Mutex
	lastAcked uint64
	aboveLast map[uint64]struct{}
	inflight  map[uint64]*Inflight
	sub       *Subscription
}

func newConsumer(id string, topic *Topic) *Consumer {
	return &Consumer{
		ID:        id,
		topic:     topic,
		aboveLast: make(map[uint64]struct{}),
		inflight:  make(map[uint64]*Inflight),
	}
}
