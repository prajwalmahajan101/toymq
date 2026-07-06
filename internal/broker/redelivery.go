package broker

import (
	"log/slog"
	"time"
)

type redeliverTask struct {
	sendCh chan<- *Inflight
	inf    *Inflight
}

func (b *Broker) runRedeliverLoop(interval time.Duration) {
	defer close(b.redeliverDone)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.sweepRedelivery(time.Now())
		case <-b.redeliverCtx.Done():
			return
		}
	}
}

func (b *Broker) sweepRedelivery(now time.Time) {
	b.mu.RLock()
	topics := make([]*Topic, 0, len(b.topics))
	for _, t := range b.topics {
		topics = append(topics, t)
	}
	b.mu.RUnlock()

	for _, t := range topics {
		for _, p := range t.partitions {
			p.consumersMu.RLock()
			consumers := make([]*Consumer, 0, len(p.consumers))
			for _, c := range p.consumers {
				consumers = append(consumers, c)
			}
			p.consumersMu.RUnlock()

			for _, c := range consumers {
				tasks := b.collectExpired(c, now)
				for _, task := range tasks {
					slog.Info("redelivering",
						"topic", t.name,
						"partition", p.id,
						"consumer-id", c.ID,
						"msg-id", task.inf.MsgID,
						"attempts", task.inf.Attempts,
					)
					b.metrics.IncRedelivery(t.name, task.inf.Attempts)
					select {
					case task.sendCh <- task.inf:
					default:
						// channel full, next tick will retry
					}
				}
			}
		}
	}
}

func (b *Broker) collectExpired(c *Consumer, now time.Time) []redeliverTask {
	c.mu.Lock()
	defer c.mu.Unlock()

	// A PAUSEd consumer suppresses redelivery too, not just first delivery
	// (ADR 0022) — otherwise the visibility-timeout sweep would keep pushing
	// MSGs the client explicitly asked to stop. Timers still advance on
	// RESUME because DeliveredAt is unchanged here.
	if c.sub == nil || c.paused || len(c.inflight) == 0 {
		return nil
	}

	sendCh := c.sub.sendCh
	deadline := b.visibilityTimeout

	var tasks []redeliverTask
	for _, inf := range c.inflight {
		if !inf.DeliveredAt.Add(deadline).Before(now) {
			continue
		}
		inf.Attempts++
		inf.DeliveredAt = now
		snapshot := *inf
		tasks = append(tasks, redeliverTask{sendCh: sendCh, inf: &snapshot})
	}
	return tasks
}
