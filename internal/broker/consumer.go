package broker

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Inflight is one message currently delivered to a consumer but not
// yet acked. The owning Consumer keeps the canonical record; every
// channel send is a value-copy snapshot per ADR 0007.
type Inflight struct {
	MsgID       uint64
	Topic       string
	Partition   int
	Payload     []byte
	DeliveredAt time.Time
	Attempts    int
}

// Consumer holds the per-consumer ack state for one Partition (ADR
// 0021 — pre-M4 this was per-Topic). See ADR 0011 for why hasAcked is a
// separate flag rather than overloading lastAcked == 0.
type Consumer struct {
	ID   string
	part *Partition

	mu sync.Mutex
	// hasAcked distinguishes "never acked anything" from "acked
	// MsgID 0", since uint64 can't represent the former with a
	// sentinel. Without this, restart after acking MsgID 0 would
	// look identical to a fresh consumer and trigger redelivery.
	hasAcked  bool
	lastAcked uint64
	aboveLast map[uint64]struct{}
	inflight  map[uint64]*Inflight
	sub       *Subscription

	persistDirty atomic.Bool
}

func newConsumer(id string, part *Partition) *Consumer {
	return &Consumer{
		ID:        id,
		part:      part,
		aboveLast: make(map[uint64]struct{}),
		inflight:  make(map[uint64]*Inflight),
	}
}

// Ack removes msgID from the inflight set and advances lastAcked /
// drains aboveLast as far as contiguous acks allow. Returns an error
// if msgID is not currently inflight.
func (c *Consumer) Ack(msgID uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.inflight[msgID]; !ok {
		return fmt.Errorf("ack: msg %d not in inflight for consumer %q", msgID, c.ID)
	}
	delete(c.inflight, msgID)
	if !c.hasAcked && msgID == 0 || c.hasAcked && msgID == c.lastAcked+1 {
		c.lastAcked = msgID
		c.hasAcked = true
		for {
			next := c.lastAcked + 1
			if _, ok := c.aboveLast[next]; !ok {
				break
			}
			delete(c.aboveLast, next)
			c.lastAcked = next
		}
	} else if !c.hasAcked || msgID > c.lastAcked+1 {
		c.aboveLast[msgID] = struct{}{}
	}
	c.persistDirty.Store(true)
	return nil
}

// Nack bumps Attempts and pushes a snapshot back onto sendCh for
// immediate redelivery. A full channel is not retried inline — the
// redelivery ticker covers that path. Returns an error if msgID is
// not currently inflight.
func (c *Consumer) Nack(msgID uint64, sendCh chan<- *Inflight) error {
	c.mu.Lock()
	inf, ok := c.inflight[msgID]
	if !ok {
		c.mu.Unlock()
		return fmt.Errorf("nack: msg %d not in inflight for consumer %q", msgID, c.ID)
	}
	inf.Attempts++
	c.persistDirty.Store(true)
	inf.DeliveredAt = time.Now()
	snapshot := *inf
	c.mu.Unlock()

	select {
	case sendCh <- &snapshot:
	default:
		//  channel full - leave for redeliver ticker
	}
	return nil
}
