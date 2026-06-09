package main

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

var (
	headerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("63")).
			Padding(0, 1)

	footerStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("241")).
			Padding(0, 1)

	statusStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("196")).
			Padding(0, 1)

	scrollbackStyle = lipgloss.NewStyle().
			Border(lipgloss.NormalBorder()).
			BorderForeground(lipgloss.Color("240")).
			Padding(0, 1)

	modalStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("63")).
			Padding(1, 2)
)

// View renders the model. Never mutates state.
func (m model) View() string {
	header := m.renderHeader()
	body := m.renderScrollback()
	footer := m.renderFooter()

	switch m.state {
	case statePubModal:
		return joinPanes(header, body, footer, m.renderPubModal())
	case stateSubModal:
		return joinPanes(header, body, footer, m.renderSubModal())
	}
	return joinPanes(header, body, footer, "")
}

func (m model) renderHeader() string {
	sub := "no subscription"
	if m.subActive {
		sub = fmt.Sprintf("consumer=%s topic=%s auto-ack=%v",
			m.consumerID, m.subTopicName, m.autoAck)
	}
	state := "connected"
	if m.state == stateDisconnected {
		state = "disconnected"
	}
	return headerStyle.Render(fmt.Sprintf(
		"ToyMQ TUI - %s @ %s - %s", state, m.addr, sub))
}

func (m model) renderScrollback() string {
	// Show only the last N lines that fit. Default to last 20 when
	// the terminal size hasn't been reported yet.
	n := 20
	if m.height > 10 {
		n = m.height - 8
	}
	start := 0
	if len(m.scrollback) > n {
		start = len(m.scrollback) - n
	}
	lines := strings.Join(m.scrollback[start:], "\n")
	return scrollbackStyle.Render(lines)
}

func (m model) renderFooter() string {
	if m.status != "" {
		return statusStyle.Render(m.status)
	}
	if m.state == stateDisconnected {
		return footerStyle.Render("[q] quit")
	}
	return footerStyle.Render(
		"[p] pub  [s] sub  [a] toggle auto-ack  [n] nack last  [q] quit")
}

func (m model) renderPubModal() string {
	lines := []string{
		"PUB",
		"",
		"topic:   " + m.pubTopic.View(),
		"payload: " + m.pubPayload.View(),
		"dedupe:  " + m.pubDedupe.View(),
		"",
		"Tab to cycle  -  Enter to submit  -  Esc to cancel",
	}
	return modalStyle.Render(strings.Join(lines, "\n"))
}

func (m model) renderSubModal() string {
	lines := []string{
		"SUB",
		"",
		"topic:       " + m.subTopic.View(),
		"consumer id: " + m.subConsumer.View(),
		"",
		"Tab to cycle  -  Enter to submit  -  Esc to cancel",
	}
	return modalStyle.Render(strings.Join(lines, "\n"))
}

// joinPanes stacks header / body / footer and overlays a modal if
// present.
func joinPanes(header, body, footer, modal string) string {
	stacked := lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
	if modal == "" {
		return stacked
	}
	return lipgloss.JoinVertical(lipgloss.Left, stacked, modal)
}
