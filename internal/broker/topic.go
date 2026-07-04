package broker

import (
	"fmt"
	"hash/fnv"
	"sync/atomic"
)

// Topic is a thin router over N partitions (ADR 0021). It owns no log
// of its own — each Partition owns a WAL, dedupe LRU, and consumers.
// Created lazily by Broker.openTopicLocked; never constructed directly
// outside the package.
type Topic struct {
	name       string
	partitions []*Partition

	// rr is the round-robin cursor for keyless (routing-key "-")
	// publishes. Non-deterministic by design — v3 must move this into
	// the proposer (ADR 0021 / ADR 0018).
	rr atomic.Uint64
}

func newTopic(name string, partitions []*Partition) *Topic {
	return &Topic{name: name, partitions: partitions}
}

func (t *Topic) count() int { return len(t.partitions) }

// route selects the target partition for a publish. An explicit
// partition (PUB <topic>#<n>) wins and is range-checked; otherwise a
// non-empty routing key hashes to a partition (fnv1a, stable across
// restarts); otherwise the keyless publish round-robins.
func (t *Topic) route(partition int, explicit bool, routingKey string) (*Partition, error) {
	n := len(t.partitions)
	if explicit {
		if partition < 0 || partition >= n {
			return nil, fmt.Errorf("partition %d out of range [0,%d) for topic %q", partition, n, t.name)
		}
		return t.partitions[partition], nil
	}
	if n == 1 {
		return t.partitions[0], nil
	}
	if routingKey != "" {
		h := fnv.New32a()
		// Hash writes never error.
		_, _ = h.Write([]byte(routingKey))
		return t.partitions[int(h.Sum32()%uint32(n))], nil
	}
	idx := int((t.rr.Add(1) - 1) % uint64(n))
	return t.partitions[idx], nil
}

// selectPartitions resolves a subscribe selector to the partitions the
// consumer should read. all (SUB <topic> or <topic>#*) returns every
// partition; otherwise the single range-checked partition.
func (t *Topic) selectPartitions(partition int, all bool) ([]*Partition, error) {
	if all {
		return t.partitions, nil
	}
	if partition < 0 || partition >= len(t.partitions) {
		return nil, fmt.Errorf("partition %d out of range [0,%d) for topic %q", partition, len(t.partitions), t.name)
	}
	return t.partitions[partition : partition+1], nil
}
