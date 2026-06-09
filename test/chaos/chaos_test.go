//go:build chaos

package chaos

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

const (
	defaultDuration   = 10 * time.Minute
	killInterval      = 30 * time.Second
	producerInterval  = 5 * time.Millisecond
	payloadSize       = 64
	drainPollInterval = 500 * time.Millisecond
	drainTimeout      = 30 * time.Second
	chaosTopic        = "chaos-orders"
	chaosConsumerID   = "chaos-consumer-1"
)

var brokerBinary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "toymq-chaos-build-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "chaos: mktemp: %v\n", err)
		os.Exit(2)
	}
	brokerBinary = filepath.Join(dir, "toymq")

	build := exec.Command("go", "build", "-o", brokerBinary, "../../cmd/toymq")
	build.Stderr = os.Stderr
	build.Stdout = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "chaos: go build failed: %v\n", err)
		os.RemoveAll(dir)
		os.Exit(2)
	}

	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func TestChaosSurvivesSIGKILL(t *testing.T) {
	duration := resolveDuration(t)
	t.Logf("chaos: duration=%s killInterval=%s producerInterval=%s payload=%dB",
		duration, killInterval, producerInterval, payloadSize)

	dataDir := t.TempDir()
	addr := pickFreeAddr(t)

	brokerErr := &bytes.Buffer{}
	sup := newSupervisor(brokerBinary, dataDir, addr, brokerErr)

	if err := sup.start(); err != nil {
		t.Fatalf("initial broker start: %v", err)
	}

	prod := newProducer(addr, chaosTopic, producerInterval, payloadSize, brokerErr)
	cons := newConsumer(addr, chaosTopic, chaosConsumerID, brokerErr)

	supCtx, cancelSup := context.WithCancel(context.Background())
	prodCtx, cancelProd := context.WithCancel(context.Background())
	consCtx, cancelCons := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); _ = sup.run(supCtx, killInterval) }()
	go func() { defer wg.Done(); prod.run(prodCtx) }()
	go func() { defer wg.Done(); cons.run(consCtx) }()

	// Soak.
	time.Sleep(duration)

	// 1. Stop the supervisor first so the broker stays up during drain.
	cancelSup()

	// 2. Stop the producer; snapshot what it claims is durable.
	cancelProd()

	// Give the producer goroutine a moment to exit so snapshot()
	// captures any final OK/DUP it had in-flight.
	wgProd := make(chan struct{})
	go func() {
		// Wait only on producer + supervisor; consumer still running.
		// We can't cleanly partition wg, so just sleep briefly — both
		// loops exit on their next tick once their ctx is cancelled.
		time.Sleep(200 * time.Millisecond)
		close(wgProd)
	}()
	<-wgProd

	prodStats := prod.snapshot()
	t.Logf("chaos: producer keysSent=%d dupHits=%d retries=%d uniqueOK=%d",
		prodStats.keysSent, prodStats.dupHits, prodStats.retries, uniqueCount(prodStats.okMsgIDs))

	// 3. Wait for the consumer's seen set to be a superset of producer.okMsgIDs.
	missing := waitForDrain(t, cons, prodStats.okMsgIDs)

	// 4. Stop the consumer and kill the broker for final teardown.
	cancelCons()
	if err := sup.kill(); err != nil {
		t.Logf("final supervisor kill: %v", err)
	}
	wg.Wait()

	consStats := cons.snapshot()
	totalDeliveries := 0
	for _, n := range consStats.seen {
		totalDeliveries += n
	}
	t.Logf("chaos: consumer uniqueSeen=%d totalDeliveries=%d errors=%d resubs=%d",
		len(consStats.seen), totalDeliveries, consStats.errors, consStats.resubs)
	t.Logf("chaos: supervisor restarts=%d", sup.restartCount())

	if len(missing) > 0 {
		sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
		if len(missing) > 20 {
			t.Errorf("chaos: %d MsgIDs OK'd by producer but never seen by consumer; first 20: %v",
				len(missing), missing[:20])
		} else {
			t.Errorf("chaos: %d MsgIDs OK'd by producer but never seen by consumer: %v",
				len(missing), missing)
		}
		// Dump broker stderr to help triage.
		if brokerErr.Len() > 0 {
			t.Logf("chaos: tail of broker stderr:\n%s", tailBytes(brokerErr.Bytes(), 4096))
		}
	}
}

func resolveDuration(t *testing.T) time.Duration {
	t.Helper()
	raw := os.Getenv("CHAOS_DURATION")
	if raw == "" {
		return defaultDuration
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("invalid CHAOS_DURATION %q: %v", raw, err)
	}
	return d
}

func pickFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick free port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// waitForDrain polls the consumer's seen map until producer.okMsgIDs
// is a subset, or drainTimeout fires. Returns the set of missing
// MsgIDs (empty on success).
func waitForDrain(t *testing.T, cons *consumer, okIDs []uint64) []uint64 {
	t.Helper()
	want := make(map[uint64]struct{}, len(okIDs))
	for _, id := range okIDs {
		want[id] = struct{}{}
	}

	deadline := time.Now().Add(drainTimeout)
	for {
		snap := cons.snapshot()
		missing := missingIDs(want, snap.seen)
		if len(missing) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return missing
		}
		t.Logf("chaos: drain waiting on %d MsgIDs (poll %s)", len(missing), drainPollInterval)
		time.Sleep(drainPollInterval)
	}
}

func missingIDs(want map[uint64]struct{}, seen map[uint64]int) []uint64 {
	out := make([]uint64, 0)
	for id := range want {
		if _, ok := seen[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

func uniqueCount(ids []uint64) int {
	set := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return len(set)
}

func tailBytes(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return "...\n" + string(b[len(b)-n:])
}
