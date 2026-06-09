package main

import (
	"errors"
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/prajwalmahajan101/toymq/pkg/client"
)

// Init is required by tea.Model. No initial Cmd — we wait for input.
func (m model) Init() tea.Cmd { return nil }

// Update is the only place model state mutates. It returns the next
// model and an optional Cmd; all blocking I/O happens inside Cmds, not
// here.
func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)

	case pubResultMsg:
		if msg.err != nil {
			m.log(fmt.Sprintf("PUB %s err=%v", msg.topic, msg.err))
		} else if msg.dup {
			m.log(fmt.Sprintf("PUB %s dup msg_id=%d", msg.topic, msg.msgID))
		} else {
			m.log(fmt.Sprintf("PUB %s ok msg_id=%d", msg.topic, msg.msgID))
		}
		return m, nil

	case subStartedMsg:
		if msg.err != nil {
			m.log(fmt.Sprintf("SUB %s err=%v", msg.topic, msg.err))
			return m, nil
		}
		m.subActive = true
		m.consumerID = msg.consumerID
		m.subTopicName = msg.topic
		m.deliveryCh = msg.ch
		m.log(fmt.Sprintf("SUB %s ok consumer=%s", msg.topic, msg.consumerID))
		return m, readDeliveryCmd(msg.ch, m.client.Err)

	case msgArrivedMsg:
		d := msg.d
		m.lastDelivery = &d
		m.log(fmt.Sprintf("MSG topic=%s id=%d payload=%q",
			d.Topic, d.MsgID, string(d.Payload)))
		var cmds []tea.Cmd
		if m.autoAck {
			cmds = append(cmds, ackCmd(m.ctx, d))
		}
		cmds = append(cmds, readDeliveryCmd(m.deliveryCh, m.client.Err))
		return m, tea.Batch(cmds...)

	case ackResultMsg:
		if msg.err != nil {
			m.log(fmt.Sprintf("%s id=%d err=%v", msg.verb, msg.msgID, msg.err))
		} else {
			m.log(fmt.Sprintf("  -> %s ok id=%d", msg.verb, msg.msgID))
		}
		return m, nil

	case transportLostMsg:
		m.state = stateDisconnected
		m.transportErr = msg.err
		if msg.err != nil && errors.Is(msg.err, client.ErrTransport) {
			m.log(fmt.Sprintf("disconnected: %v", msg.err))
		} else {
			m.log("disconnected")
		}
		return m, nil
	}

	// In modal states forward unhandled messages to the focused input
	// so character input updates the textinput model.
	return m.forwardToInput(msg)
}

// handleKey dispatches a key press based on the current state.
func (m model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch m.state {
	case statePubModal:
		return m.handlePubKey(msg)
	case stateSubModal:
		return m.handleSubKey(msg)
	case stateDisconnected:
		if msg.String() == "q" || msg.Type == tea.KeyCtrlC {
			return m, tea.Quit
		}
		return m, nil
	}

	// stateMain
	switch msg.String() {
	case "q", "ctrl+c":
		return m, tea.Quit
	case "p":
		m.state = statePubModal
		m.resetPubModal()
		return m, textinput.Blink
	case "s":
		if m.subActive {
			m.status = "already subscribed; restart to change consumer"
			return m, nil
		}
		m.state = stateSubModal
		m.resetSubModal()
		return m, textinput.Blink
	case "a":
		m.autoAck = !m.autoAck
		m.log(fmt.Sprintf("auto-ack = %v", m.autoAck))
		return m, nil
	case "n":
		if m.lastDelivery == nil {
			m.status = "no MSG to nack"
			return m, nil
		}
		d := *m.lastDelivery
		m.log(fmt.Sprintf("NACK id=%d ...", d.MsgID))
		return m, nackCmd(m.ctx, d)
	}
	return m, nil
}

// handlePubKey runs in statePubModal. Tab cycles fields, Esc cancels,
// Enter submits when on the last field.
func (m model) handlePubKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.state = stateMain
		return m, nil
	case "tab", "down":
		m.pubFocus = (m.pubFocus + 1) % 3
		m.focusPubField()
		return m, nil
	case "shift+tab", "up":
		m.pubFocus = (m.pubFocus + 2) % 3
		m.focusPubField()
		return m, nil
	case "enter":
		topic := m.pubTopic.Value()
		payload := m.pubPayload.Value()
		dedupe := m.pubDedupe.Value()
		if topic == "" || payload == "" {
			m.status = "pub: topic and payload required"
			return m, nil
		}
		m.state = stateMain
		m.status = ""
		m.log(fmt.Sprintf("PUB %s ...", topic))
		return m, pubCmd(m.ctx, m.client, topic, dedupe, payload)
	}
	return m.forwardToInput(msg)
}

// handleSubKey runs in stateSubModal.
func (m model) handleSubKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.state = stateMain
		return m, nil
	case "tab", "down":
		m.subFocus = (m.subFocus + 1) % 2
		m.focusSubField()
		return m, nil
	case "shift+tab", "up":
		m.subFocus = (m.subFocus + 1) % 2
		m.focusSubField()
		return m, nil
	case "enter":
		topic := m.subTopic.Value()
		cid := m.subConsumer.Value()
		if topic == "" || cid == "" {
			m.status = "sub: topic and consumer-id required"
			return m, nil
		}
		m.state = stateMain
		m.status = ""
		m.log(fmt.Sprintf("SUB %s consumer=%s ...", topic, cid))
		return m, subCmd(m.ctx, m.client, topic, cid)
	}
	return m.forwardToInput(msg)
}

func (m *model) focusPubField() {
	m.pubTopic.Blur()
	m.pubPayload.Blur()
	m.pubDedupe.Blur()
	switch m.pubFocus {
	case 0:
		m.pubTopic.Focus()
	case 1:
		m.pubPayload.Focus()
	case 2:
		m.pubDedupe.Focus()
	}
}

func (m *model) focusSubField() {
	m.subTopic.Blur()
	m.subConsumer.Blur()
	switch m.subFocus {
	case 0:
		m.subTopic.Focus()
	case 1:
		m.subConsumer.Focus()
	}
}

// forwardToInput routes character input to the focused textinput.
func (m model) forwardToInput(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch m.state {
	case statePubModal:
		switch m.pubFocus {
		case 0:
			m.pubTopic, cmd = m.pubTopic.Update(msg)
		case 1:
			m.pubPayload, cmd = m.pubPayload.Update(msg)
		case 2:
			m.pubDedupe, cmd = m.pubDedupe.Update(msg)
		}
	case stateSubModal:
		switch m.subFocus {
		case 0:
			m.subTopic, cmd = m.subTopic.Update(msg)
		case 1:
			m.subConsumer, cmd = m.subConsumer.Update(msg)
		}
	}
	return m, cmd
}
