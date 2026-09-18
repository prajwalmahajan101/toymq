package main

import (
	"context"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/prajwalmahajan101/toymq/pkg/client"
)

// freshModel builds a model with no live client. Pure-Update tests
// must not call any tea.Cmd that touches the client. A typed-nil
// *client.Client (not an untyped nil) is passed so method values like
// m.client.Err resolve without dereferencing a nil interface.
func freshModel(t *testing.T) model {
	t.Helper()
	return newModel(context.Background(), (*client.Client)(nil), "127.0.0.1:0")
}

func key(s string) tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
}

func keyType(k tea.KeyType) tea.KeyMsg { return tea.KeyMsg{Type: k} }

func TestUpdate_PKeyOpensPubModal(t *testing.T) {
	m := freshModel(t)
	next, _ := m.Update(key("p"))
	got := next.(model)
	if got.state != statePubModal {
		t.Fatalf("state = %v, want statePubModal", got.state)
	}
	if !got.pubTopic.Focused() {
		t.Fatalf("pub modal opened but topic field not focused")
	}
}

func TestUpdate_SKeyOpensSubModal(t *testing.T) {
	m := freshModel(t)
	next, _ := m.Update(key("s"))
	got := next.(model)
	if got.state != stateSubModal {
		t.Fatalf("state = %v, want stateSubModal", got.state)
	}
}

func TestUpdate_SKeyBlockedWhenSubActive(t *testing.T) {
	m := freshModel(t)
	m.subActive = true
	next, _ := m.Update(key("s"))
	got := next.(model)
	if got.state != stateMain {
		t.Fatalf("state = %v, want stateMain (sub blocked)", got.state)
	}
	if !strings.Contains(got.status, "already subscribed") {
		t.Fatalf("status = %q, want already-subscribed warning", got.status)
	}
}

func TestUpdate_EscClosesPubModal(t *testing.T) {
	m := freshModel(t)
	m.state = statePubModal
	next, _ := m.Update(keyType(tea.KeyEsc))
	got := next.(model)
	if got.state != stateMain {
		t.Fatalf("Esc didn't return to main, got %v", got.state)
	}
}

func TestUpdate_EscClosesSubModal(t *testing.T) {
	m := freshModel(t)
	m.state = stateSubModal
	next, _ := m.Update(keyType(tea.KeyEsc))
	got := next.(model)
	if got.state != stateMain {
		t.Fatalf("Esc didn't return to main, got %v", got.state)
	}
}

func TestUpdate_AToggleAutoAck(t *testing.T) {
	m := freshModel(t)
	if !m.autoAck {
		t.Fatalf("default autoAck = false, want true")
	}
	next, _ := m.Update(key("a"))
	got := next.(model)
	if got.autoAck {
		t.Fatalf("autoAck did not toggle off")
	}
	next2, _ := got.Update(key("a"))
	if !next2.(model).autoAck {
		t.Fatalf("autoAck did not toggle back on")
	}
}

func TestUpdate_NKeyNoLastDelivery(t *testing.T) {
	m := freshModel(t)
	next, cmd := m.Update(key("n"))
	got := next.(model)
	if cmd != nil {
		t.Fatalf("nack without last delivery should not dispatch a Cmd")
	}
	if !strings.Contains(got.status, "no MSG to nack") {
		t.Fatalf("status = %q, want no-MSG warning", got.status)
	}
}

func TestUpdate_TransportLostSwitchesState(t *testing.T) {
	m := freshModel(t)
	next, _ := m.Update(transportLostMsg{err: client.ErrTransport})
	got := next.(model)
	if got.state != stateDisconnected {
		t.Fatalf("state = %v, want stateDisconnected", got.state)
	}
}

func TestUpdate_DisconnectedQKeyQuits(t *testing.T) {
	m := freshModel(t)
	m.state = stateDisconnected
	_, cmd := m.Update(key("q"))
	if cmd == nil {
		t.Fatalf("q in disconnected state must return tea.Quit")
	}
	// tea.Quit is the package-level Cmd; we can't compare function
	// pointers, but we can confirm it returns a tea.QuitMsg when
	// invoked.
	msg := cmd()
	if _, ok := msg.(tea.QuitMsg); !ok {
		t.Fatalf("cmd returned %T, want tea.QuitMsg", msg)
	}
}

func TestUpdate_PubResultLogged(t *testing.T) {
	m := freshModel(t)
	next, _ := m.Update(pubResultMsg{topic: "t", msgID: 42})
	got := next.(model)
	if !strings.Contains(got.scrollback[len(got.scrollback)-1], "msg_id=42") {
		t.Fatalf("last scrollback line = %q, missing msg_id=42",
			got.scrollback[len(got.scrollback)-1])
	}
}

func TestUpdate_SubStartedActivatesSub(t *testing.T) {
	m := freshModel(t)
	ch := make(chan client.Delivery, 1)
	next, _ := m.Update(subStartedMsg{
		topic:      "orders",
		consumerID: "c1",
		ch:         ch,
	})
	got := next.(model)
	if !got.subActive {
		t.Fatalf("subActive = false after subStartedMsg success")
	}
	if got.consumerID != "c1" || got.subTopicName != "orders" {
		t.Fatalf("sub fields not set: %+v", got)
	}
}

func TestUpdate_MsgArrivedAutoAcks(t *testing.T) {
	m := freshModel(t)
	ch := make(chan client.Delivery, 1)
	m.deliveryCh = ch
	d := client.Delivery{
		Topic:   "t",
		MsgID:   7,
		Payload: []byte("hi"),
		Ack:     func(context.Context) error { return nil },
		Nack:    func(context.Context) error { return nil },
	}
	next, cmd := m.Update(msgArrivedMsg{d: d})
	got := next.(model)
	if got.lastDelivery == nil || got.lastDelivery.MsgID != 7 {
		t.Fatalf("lastDelivery not stored")
	}
	if cmd == nil {
		t.Fatalf("expected a Batch Cmd (ack + next read), got nil")
	}
}

func TestUpdate_PubModalTabReturnsCmd(t *testing.T) {
	m := freshModel(t)
	// Open the pub modal.
	next, _ := m.Update(key("p"))
	opened := next.(model)
	if opened.state != statePubModal || opened.pubFocus != 0 {
		t.Fatalf("setup: state=%v focus=%d, want pubModal/0",
			opened.state, opened.pubFocus)
	}
	// Tab to the next field. The handler must return a non-nil Cmd
	// (the textinput blink) so the cursor keeps animating on the
	// newly focused payload field.
	next, cmd := opened.Update(keyType(tea.KeyTab))
	got := next.(model)
	if got.pubFocus != 1 {
		t.Fatalf("pubFocus = %d, want 1 after Tab", got.pubFocus)
	}
	if cmd == nil {
		t.Fatalf("Tab returned nil Cmd; textinput.Focus() blink Cmd dropped")
	}
}

func TestUpdate_TransportLostClearsLastDelivery(t *testing.T) {
	m := freshModel(t)
	ch := make(chan client.Delivery, 1)
	m.deliveryCh = ch
	m.lastDelivery = &client.Delivery{MsgID: 7}

	next, _ := m.Update(transportLostMsg{err: client.ErrTransport})
	got := next.(model)

	if got.state != stateDisconnected {
		t.Fatalf("state = %v, want stateDisconnected", got.state)
	}
	if got.lastDelivery != nil {
		t.Fatalf("lastDelivery = %+v, want nil after transport loss",
			got.lastDelivery)
	}
	if got.deliveryCh != nil {
		t.Fatalf("deliveryCh not cleared after transport loss")
	}
}

func TestUpdate_CKeyOpensClusterView(t *testing.T) {
	m := freshModel(t)
	next, cmd := m.Update(key("c"))
	got := next.(model)
	if got.state != stateClusterView {
		t.Fatalf("state = %v, want stateClusterView", got.state)
	}
	if got.pollGen != 1 {
		t.Fatalf("pollGen = %d, want 1 (fresh poll chain)", got.pollGen)
	}
	if cmd == nil {
		t.Fatalf("expected an initial poll+tick cmd, got nil")
	}
}

func TestUpdate_ClusterEscReturnsMain(t *testing.T) {
	m := freshModel(t)
	m.state = stateClusterView
	m.pollGen = 1
	next, _ := m.Update(key("c")) // toggle key also exits
	got := next.(model)
	if got.state != stateMain {
		t.Fatalf("state = %v, want stateMain after esc/c", got.state)
	}
	if got.pollGen != 2 {
		t.Fatalf("pollGen = %d, want 2 (bumped so the ticker self-expires)", got.pollGen)
	}
}

func TestUpdate_ClusterInfoStoredAndStaleDropped(t *testing.T) {
	m := freshModel(t)
	m.pollGen = 3
	info := client.ReplicationInfo{Role: "leader", Raw: map[string]string{"role": "leader"}}
	next, _ := m.Update(clusterInfoMsg{info: info, gen: 3})
	got := next.(model)
	if got.clusterInfo.Role != "leader" {
		t.Fatalf("clusterInfo.Role = %q, want leader", got.clusterInfo.Role)
	}

	stale := client.ReplicationInfo{Role: "follower", Raw: map[string]string{"role": "follower"}}
	next2, _ := got.Update(clusterInfoMsg{info: stale, gen: 2})
	got2 := next2.(model)
	if got2.clusterInfo.Role != "leader" {
		t.Fatalf("stale gen applied: clusterInfo.Role = %q, want leader (unchanged)", got2.clusterInfo.Role)
	}
}

func TestUpdate_ClusterPollTickReArmsOnlyWhenOpen(t *testing.T) {
	m := freshModel(t)
	m.state = stateClusterView
	m.pollGen = 1
	if _, cmd := m.Update(clusterPollTickMsg{gen: 1}); cmd == nil {
		t.Fatalf("open view + matching gen: expected re-arm cmd, got nil")
	}
	// Left the view: a lingering tick must not re-arm.
	m.state = stateMain
	if _, cmd := m.Update(clusterPollTickMsg{gen: 1}); cmd != nil {
		t.Fatalf("view left: expected nil cmd (chain stops), got a cmd")
	}
	// Stale gen must not re-arm even while open.
	m.state = stateClusterView
	if _, cmd := m.Update(clusterPollTickMsg{gen: 0}); cmd != nil {
		t.Fatalf("stale gen: expected nil cmd, got a cmd")
	}
}

// TestUpdate_ClusterLeaderChangeReflected is the M5 owned-risk test: a
// leader change on the polled node must show in the rendered view.
func TestUpdate_ClusterLeaderChangeReflected(t *testing.T) {
	m := freshModel(t)
	m.width, m.height = 100, 30
	m.pollGen = 1
	m.state = stateClusterView

	asLeader := client.ReplicationInfo{Role: "leader", Raw: map[string]string{
		"role": "leader", "leader": "n1", "term": "5",
		"commit_index": "42", "apply_index": "42", "last_log_index": "42",
		"connected_replicas":     "2",
		"replica_n2_match_index": "42", "replica_n2_lag_entries": "0",
		"replica_n3_match_index": "40", "replica_n3_lag_entries": "2",
	}}
	next, _ := m.Update(clusterInfoMsg{info: asLeader, gen: 1})
	m = next.(model)
	v := m.View()
	if !strings.Contains(v, "n1") || !strings.Contains(v, "n3") {
		t.Fatalf("leader view missing peers/leader:\n%s", v)
	}

	// n1 loses leadership; a re-poll now returns follower + new leader n2.
	asFollower := client.ReplicationInfo{Role: "follower", Raw: map[string]string{
		"role": "follower", "leader": "n2", "term": "6",
		"commit_index": "50", "apply_index": "50", "last_log_index": "50",
		"connected_replicas": "0",
	}}
	next2, _ := m.Update(clusterInfoMsg{info: asFollower, gen: 1})
	m = next2.(model)
	v2 := m.View()
	if !strings.Contains(v2, "follower") {
		t.Fatalf("view did not reflect role change to follower:\n%s", v2)
	}
	if !strings.Contains(v2, "n2") {
		t.Fatalf("view did not reflect new leader n2:\n%s", v2)
	}
}

func TestLog_TrimsToMaxScrollback(t *testing.T) {
	m := freshModel(t)
	for i := 0; i < maxScrollback+50; i++ {
		m.log("line")
	}
	if len(m.scrollback) != maxScrollback {
		t.Fatalf("scrollback len = %d, want %d",
			len(m.scrollback), maxScrollback)
	}
}
