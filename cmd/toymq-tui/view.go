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
//
// Layout: header / scrollback / (modal?) / footer stacked vertically.
// When a pub or sub modal is active it occupies its natural height
// between the (shrunk) scrollback and the footer, so the log lines
// above remain visible while the form is open.
func (m model) View() string {
	header := m.renderHeader()
	footer := m.renderFooter()
	headerH := lipgloss.Height(header)
	footerH := lipgloss.Height(footer)

	var modal string
	switch m.state {
	case statePubModal:
		modal = m.renderPubModal()
	case stateSubModal:
		modal = m.renderSubModal()
	}

	if modal == "" {
		body := m.renderScrollback(headerH, footerH)
		return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
	}

	w := m.width
	if w <= 0 {
		w = 80
	}
	modalH := lipgloss.Height(modal)
	centered := lipgloss.PlaceHorizontal(w, lipgloss.Center, modal)
	body := m.renderScrollback(headerH, footerH+modalH)
	return lipgloss.JoinVertical(lipgloss.Left, header, body, centered, footer)
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
	line := fmt.Sprintf("ToyMQ TUI - %s @ %s - %s", state, m.addr, sub)
	w := m.width
	if w <= 0 {
		w = 80
	}
	return headerStyle.Width(w).Render(line)
}

// renderScrollback sizes the bordered pane to fill the terminal between
// the header and footer. When the terminal size hasn't arrived yet
// (zero width/height), fall back to a reasonable default.
func (m model) renderScrollback(headerH, footerH int) string {
	w, h := m.width, m.height
	if w <= 0 {
		w = 80
	}
	if h <= 0 {
		h = 24
	}
	// inner sizes account for border (2) and padding (2).
	innerW := w - 4
	innerH := h - headerH - footerH - 2
	if innerW < 10 {
		innerW = 10
	}
	if innerH < 3 {
		innerH = 3
	}

	start := 0
	if len(m.scrollback) > innerH {
		start = len(m.scrollback) - innerH
	}
	visible := m.scrollback[start:]

	// Pad the bottom with blank lines so the border draws around the
	// full pane instead of shrinking to content.
	for len(visible) < innerH {
		visible = append(visible, "")
	}
	lines := strings.Join(visible, "\n")

	return scrollbackStyle.
		Width(innerW + 2).
		Height(innerH).
		Render(lines)
}

func (m model) renderFooter() string {
	w := m.width
	if w <= 0 {
		w = 80
	}
	if m.status != "" {
		return statusStyle.Width(w).Render(m.status)
	}
	if m.state == stateDisconnected {
		return footerStyle.Width(w).Render("[q] quit")
	}
	return footerStyle.Width(w).Render(
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

