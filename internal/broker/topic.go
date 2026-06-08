package broker

import (
	"context"
	"sync"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/wal"
)

type Subscription struct {
	consumerID string
	cancel     context.CancelFunc
	done       chan struct{}
}

type Topic struct {
	name   string
	log    *wal.Log
	dedupe *DedupeIndex

	pubMu     sync.Mutex
	consumers map[string]*Consumer
}

func newTopic(name string, log *wal.Log, dedupeCap int) *Topic {
	return &Topic{
		name:      name,
		log:       log,
		dedupe:    NewDedupeIndex(dedupeCap),
		consumers: make(map[string]*Consumer),
	}
}

func (t *Topic) Publish(key string, payload []byte) (msgID uint64, duplicate bool, err error) {
	t.pubMu.Lock()
	defer t.pubMu.Unlock()

	if key != "" {
		if existing, ok := t.dedupe.Lookup(key); ok {
			return existing, true, nil
		}
	}

	rec := wal.Record{
		TsNs:      uint64(time.Now().UnixNano()),
		DedupeKey: key,
		Payload:   payload,
	}

	id, _, err := t.log.Append(rec)
	if err != nil {
		return 0, false, err
	}
	if key != "" {
		t.dedupe.Insert(key, id)
	}

	return id, false, nil
}
