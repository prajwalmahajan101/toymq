package integration

import "testing"

// TestInfoReplicationStandalone: a non-replicated broker answers INFO
// replication with role:standalone and nothing else (v3 M4, ADR 0033).
func TestInfoReplicationStandalone(t *testing.T) {
	h := startBroker(t)
	c := dial(t, h.addr)

	info := c.info(t)
	if got := info["role"]; got != "standalone" {
		t.Fatalf("role = %q, want standalone", got)
	}
	if len(info) != 1 {
		t.Fatalf("standalone INFO should have exactly 1 line, got %d: %v", len(info), info)
	}
}

// TestPubWaitStandaloneNoop: PUB … WAIT on a standalone broker is a no-op
// barrier — it returns a normal OK (v3 M4, ADR 0033).
func TestPubWaitStandaloneNoop(t *testing.T) {
	h := startBroker(t)
	c := dial(t, h.addr)

	c.pubWait(t, "orders", []byte("hello"), 3, 100)
	_ = c.expectOK(t)
}
