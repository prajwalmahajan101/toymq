package broker

import (
	"context"
	"log/slog"
	"strings"

	"github.com/prajwalmahajan101/toymq/internal/tracing"
)

// dlqSuffix names the auto-created dead-letter topic for a source topic:
// orders -> orders.dlq. A topic already ending in it is exempt from the
// DLQ (no orders.dlq.dlq), so poison messages there redeliver normally.
const dlqSuffix = ".dlq"

// isDLQTopic reports whether name is itself a dead-letter topic.
func isDLQTopic(name string) bool {
	return strings.HasSuffix(name, dlqSuffix)
}

// dlqThreshold returns the effective dead-letter attempt limit for
// topic: the configured --dlq-after-nacks, or 0 (disabled) when the DLQ
// is off globally or topic is itself a .dlq topic (the loop guard).
func (b *Broker) dlqThreshold(topic string) int {
	if b.dlqAfterNacks <= 0 || isDLQTopic(topic) {
		return 0
	}
	return b.dlqAfterNacks
}

// dlqMove republishes a dead message's payload onto <srcTopic>.dlq — the
// single deterministic seam for dead-lettering (ADR 0024). The caller
// has already removed the message from the source consumer's inflight (a
// synthetic ack), so this only appends to the dead-letter log; consumers
// SUB <topic>.dlq to inspect it with all the normal machinery.
//
// The .dlq topic is created lazily with a single partition. In v3 the
// leader detects the threshold and proposes this move; every node then
// applies the append deterministically (the in-memory Attempts count is
// the v2 trigger input only).
func (b *Broker) dlqMove(srcTopic string, payload []byte) error {
	return b.dlqMoveCtx(context.Background(), srcTopic, payload)
}

// dlqMoveCtx is dlqMove with a context carrying the caller's span; it
// records a "broker.dlq_move" child span (ADR 0027) around the republish.
func (b *Broker) dlqMoveCtx(ctx context.Context, srcTopic string, payload []byte) error {
	dlqTopic := srcTopic + dlqSuffix
	ctx, span := b.startSpan(ctx, "broker.dlq_move",
		tracing.AttrTopic.String(dlqTopic),
		tracing.AttrPayloadBytes.Int(len(payload)),
	)
	defer span.End()

	// Ensure a single-partition .dlq topic. A pre-existing topic with a
	// different count returns an error from CreateTopic; ignore it and
	// publish to the existing topic rather than failing the move.
	_ = b.CreateTopic(dlqTopic, 1)

	_, _, _, err := b.PublishCtx(ctx, dlqTopic, "", "", 0, false, payload, 0)
	if err != nil {
		slog.Error("dlq move", "src-topic", srcTopic, "dlq-topic", dlqTopic, "err", err)
	}
	return err
}
