package main

import (
	"fmt"
	"sort"
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

	if m.state == stateClusterView {
		body := m.renderClusterView(headerH, footerH)
		return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
	}

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

// renderClusterView renders the live cluster pane (v3 M5) from the last
// INFO replication poll. It reuses the scrollback frame. Peer rows are
// leader-only (Status().MatchIndex is nil on a follower), so a follower
// shows its role + leader hint + offsets, and standalone shows a notice.
func (m model) renderClusterView(headerH, footerH int) string {
	w, h := m.width, m.height
	if w <= 0 {
		w = 80
	}
	if h <= 0 {
		h = 24
	}
	innerW := w - 4
	innerH := h - headerH - footerH - 2
	if innerW < 10 {
		innerW = 10
	}
	if innerH < 3 {
		innerH = 3
	}

	var lines []string
	raw := m.clusterInfo.Raw
	role := m.clusterInfo.Role

	switch {
	case m.clusterErr != nil:
		lines = append(lines, "cluster: "+m.clusterErr.Error())
	case len(raw) == 0:
		lines = append(lines, "polling INFO replication ...")
	case role == "standalone":
		lines = append(lines, "standalone - no cluster (single node, raft disabled)")
	default:
		lines = append(lines,
			"role:           "+raw["role"],
			"leader:         "+raw["leader"],
			"term:           "+raw["term"],
			"commit_index:   "+raw["commit_index"],
			"apply_index:    "+raw["apply_index"],
			"last_log_index: "+raw["last_log_index"],
			"",
		)
		peers := peerRows(raw)
		if len(peers) == 0 {
			lines = append(lines,
				fmt.Sprintf("peer detail is leader-only; this node is %s (leader=%s)",
					raw["role"], raw["leader"]))
		} else {
			lines = append(lines,
				fmt.Sprintf("replicas (%s):", raw["connected_replicas"]),
				fmt.Sprintf("  %-16s %14s %10s", "PEER", "MATCH_INDEX", "LAG"))
			lines = append(lines, peers...)
		}
	}

	for len(lines) < innerH {
		lines = append(lines, "")
	}
	if len(lines) > innerH {
		lines = lines[:innerH]
	}

	return scrollbackStyle.
		Width(innerW + 2).
		Height(innerH).
		Render(strings.Join(lines, "\n"))
}

// peerRows builds sorted, aligned per-peer rows from the flattened INFO
// keys replica_<id>_match_index / replica_<id>_lag_entries.
func peerRows(raw map[string]string) []string {
	ids := make([]string, 0)
	for k := range raw {
		if id, ok := strings.CutPrefix(k, "replica_"); ok {
			if id, ok := strings.CutSuffix(id, "_match_index"); ok {
				ids = append(ids, id)
			}
		}
	}
	sort.Strings(ids)
	rows := make([]string, 0, len(ids))
	for _, id := range ids {
		rows = append(rows, fmt.Sprintf("  %-16s %14s %10s",
			id, raw["replica_"+id+"_match_index"], raw["replica_"+id+"_lag_entries"]))
	}
	return rows
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
	if m.state == stateClusterView {
		return footerStyle.Width(w).Render("[esc/c] back  [q] quit")
	}
	return footerStyle.Width(w).Render(
		"[p] pub  [s] sub  [a] toggle auto-ack  [n] nack last  [c] cluster  [q] quit")
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
