package main

import (
	"context"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/prajwalmahajan101/toymq/pkg/client"
)

// pubCmd dispatches a PUB on a background goroutine and returns a
// pubResultMsg when the broker responds.
func pubCmd(ctx context.Context, c *client.Client, topic, dedupe, payload string) tea.Cmd {
	return func() tea.Msg {
		callCtx, cancel := context.WithTimeout(ctx, opTimeout)
		defer cancel()
		id, dup, err := c.Pub(callCtx, topic, dedupe, []byte(payload))
		return pubResultMsg{topic: topic, msgID: id, dup: dup, err: err}
	}
}

// subCmd opens a subscription. On success the returned channel is
// handed back via subStartedMsg; the caller is responsible for
// chaining a readDeliveryCmd to pump it.
func subCmd(ctx context.Context, c *client.Client, topic, consumerID string) tea.Cmd {
	return func() tea.Msg {
		ch, err := c.Sub(ctx, topic, consumerID)
		return subStartedMsg{topic: topic, consumerID: consumerID, ch: ch, err: err}
	}
}

// readDeliveryCmd blocks until one delivery arrives on ch, then
// returns it as msgArrivedMsg. If ch closes, returns transportLostMsg.
// The Update handler is expected to chain another readDeliveryCmd for
// the next delivery — at most one of these goroutines is alive at any
// time.
func readDeliveryCmd(ch <-chan client.Delivery, errFn func() error) tea.Cmd {
	return func() tea.Msg {
		d, ok := <-ch
		if !ok {
			return transportLostMsg{err: errFn()}
		}
		return msgArrivedMsg{d: d}
	}
}

// ackCmd runs Delivery.Ack on a background goroutine.
func ackCmd(ctx context.Context, d client.Delivery) tea.Cmd {
	return func() tea.Msg {
		callCtx, cancel := context.WithTimeout(ctx, opTimeout)
		defer cancel()
		err := d.Ack(callCtx)
		return ackResultMsg{verb: "ack", msgID: d.MsgID, err: err}
	}
}

// nackCmd runs Delivery.Nack on a background goroutine.
func nackCmd(ctx context.Context, d client.Delivery) tea.Cmd {
	return func() tea.Msg {
		callCtx, cancel := context.WithTimeout(ctx, opTimeout)
		defer cancel()
		err := d.Nack(callCtx)
		return ackResultMsg{verb: "nack", msgID: d.MsgID, err: err}
	}
}
