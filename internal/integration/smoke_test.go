package integration

import (
	"runtime"
	"testing"
	"time"
)

func TestSmokePubOK(t *testing.T) {
	h := startBroker(t)
	c := dial(t, h.addr)

	c.pub(t, "orders", "", []byte("hello"))
	// First published message in a fresh broker gets MsgID 0; just
	// confirm the wire round-trip returned an OK.
	_ = c.expectOK(t)
}

func TestHarnessGoroutineBaseline(t *testing.T) {
	baseline := runtime.NumGoroutine()
	for i := 0; i < 5; i++ {
		h := startBroker(t)
		c := dial(t, h.addr)
		c.pub(t, "orders", "", []byte("payload"))
		_ = c.expectOK(t)
		c.close()
		h.shutdown(t)
	}

	deadline := time.Now().Add(time.Second)
	for runtime.NumGoroutine() > baseline+2 {
		if time.Now().After(deadline) {
			t.Fatalf("goroutine count did not return to baseline: have %d, baseline %d", runtime.NumGoroutine(), baseline)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
