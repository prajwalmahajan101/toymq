package main

import (
	"context"
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	"github.com/prajwalmahajan101/toymq/pkg/client"
)

const (
	maxScrollback = 1000
	opTimeout     = 5 * time.Second
)

// state is the top-level UI state. Modal states own keyboard focus
// until they emit a submit or cancel message.
type state int

const (
	stateMain state = iota
	statePubModal
	stateSubModal
	stateDisconnected
)

// model is the root tea.Model. Held by value; Update returns a new
// model with the mutated fields. Pointers (client, ctx) are shared by
// design — the TUI owns exactly one client for its lifetime.
type model struct {
	ctx    context.Context
	client *client.Client
	addr   string

	state state

	// scrollback is a ring of recent log lines. log() appends and
	// trims to maxScrollback.
	scrollback []string

	// pub modal
	pubTopic   textinput.Model
	pubPayload textinput.Model
	pubDedupe  textinput.Model
	pubFocus   int // 0 topic, 1 payload, 2 dedupe

	// sub modal
	subTopic    textinput.Model
	subConsumer textinput.Model
	subFocus    int // 0 topic, 1 consumer

	// subscription state
	subActive    bool
	consumerID   string
	subTopicName string
	autoAck      bool
	// deliveryCh is the channel returned by Client.Sub. Stored so the
	// recursive readDeliveryCmd chain can pull the next delivery
	// after each msgArrivedMsg.
	deliveryCh <-chan client.Delivery

	// nack-last: the most recent delivery, for the `n` shortcut.
	lastDelivery *client.Delivery

	// status line shown in the footer (transient errors).
	status string

	// transportErr captures the error that closed the conn, if any.
	transportErr error

	width, height int
}

// newModel builds a fresh model bound to an already-dialed client.
// ctx is the program-level context (cancelled on SIGINT/SIGTERM).
func newModel(ctx context.Context, c *client.Client, addr string) model {
	mk := func(placeholder string, width int) textinput.Model {
		ti := textinput.New()
		ti.Placeholder = placeholder
		ti.CharLimit = 4096
		ti.Width = width
		return ti
	}

	m := model{
		ctx:         ctx,
		client:      c,
		addr:        addr,
		state:       stateMain,
		autoAck:     true,
		pubTopic:    mk("orders", 32),
		pubPayload:  mk("hello", 48),
		pubDedupe:   mk("(optional)", 32),
		subTopic:    mk("orders", 32),
		subConsumer: mk("c1", 32),
	}
	m.log(fmt.Sprintf("connected to %s", addr))
	return m
}

// log appends one line to the scrollback, trimming to maxScrollback.
// The timestamp is RFC3339 seconds-precision; the line itself is
// caller-formatted.
func (m *model) log(line string) {
	stamped := fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), line)
	m.scrollback = append(m.scrollback, stamped)
	if len(m.scrollback) > maxScrollback {
		m.scrollback = m.scrollback[len(m.scrollback)-maxScrollback:]
	}
}

// resetPubModal clears the modal inputs and resets focus.
func (m *model) resetPubModal() {
	m.pubTopic.SetValue("")
	m.pubPayload.SetValue("")
	m.pubDedupe.SetValue("")
	m.pubFocus = 0
	m.pubTopic.Focus()
	m.pubPayload.Blur()
	m.pubDedupe.Blur()
}

// resetSubModal clears the sub modal inputs and resets focus.
func (m *model) resetSubModal() {
	m.subTopic.SetValue("")
	m.subConsumer.SetValue("")
	m.subFocus = 0
	m.subTopic.Focus()
	m.subConsumer.Blur()
}
