package broker

import (
	"fmt"
	"sync"
	"sync/atomic"
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

	persistDirty atomic.Bool
}

func newConsumer(id string, topic *Topic) *Consumer {
	return &Consumer{
		ID:        id,
		topic:     topic,
		aboveLast: make(map[uint64]struct{}),
		inflight:  make(map[uint64]*Inflight),
	}
}

func (c *Consumer) Ack(msgID uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.inflight[msgID]; !ok {
		return fmt.Errorf("ack: msg %d not in inflight for consumer %q", msgID, c.ID)
	}
	delete(c.inflight, msgID)
	if msgID == c.lastAcked+1 || c.lastAcked == 0 && msgID == 0 {
		c.lastAcked = msgID
		for {
			next := c.lastAcked + 1
			if _, ok := c.aboveLast[next]; !ok {
				break
			}
			delete(c.aboveLast, next)
			c.lastAcked = next
		}
	} else if msgID > c.lastAcked+1 {
		c.aboveLast[msgID] = struct{}{}
	}
	c.persistDirty.Store(true)
	return nil
}

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
