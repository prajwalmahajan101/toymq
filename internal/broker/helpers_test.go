package broker

import "context"

// Single-partition test helpers preserving the pre-M4 call shape. Every
// topic in these tests is 1-partition (partition 0), so routing key is
// empty, subscribe is all-partitions, and ack/nack target partition 0.

func bpub(b *Broker, topic, key string, payload []byte) (uint64, bool, error) {
	id, _, dup, err := b.Publish(topic, key, "", 0, false, payload)
	return id, dup, err
}

func bsub(b *Broker, ctx context.Context, topic, consumerID string, ch chan *Inflight) (*Subscription, error) {
	subs, err := b.Subscribe(ctx, topic, 0, true, consumerID, ch)
	if err != nil {
		return nil, err
	}
	if len(subs) == 0 {
		return nil, nil
	}
	return subs[0], nil
}

func back(b *Broker, topic, consumerID string, msgID uint64) error {
	return b.Ack(topic, 0, consumerID, msgID)
}

func bnack(b *Broker, topic, consumerID string, msgID uint64, ch chan *Inflight) error {
	return b.Nack(topic, 0, consumerID, msgID, ch)
}

// part0 returns partition 0 of a topic — the single partition for all
// 1-partition test topics, where consumer/dedupe state now lives (ADR 0021).
func part0(t *Topic) *Partition { return t.partitions[0] }
