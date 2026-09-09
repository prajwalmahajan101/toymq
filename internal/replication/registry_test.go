package replication

import (
	"sync"
	"testing"
)

func TestRegistryRegisterResolve(t *testing.T) {
	r := NewResultRegistry()
	nonce, ch := r.Register()
	if nonce == 0 {
		t.Fatal("register returned nonce 0, which is the no-waiter sentinel")
	}
	r.resolve(nonce, ApplyResult{MsgID: 99, Dup: true})
	got := <-ch
	if got.MsgID != 99 || !got.Dup {
		t.Fatalf("got %+v, want {99 true}", got)
	}
}

func TestRegistryNonceMonotonicAndNonZero(t *testing.T) {
	r := NewResultRegistry()
	seen := map[uint64]bool{}
	for range 1000 {
		n := r.next()
		if n == 0 {
			t.Fatal("next() returned 0")
		}
		if seen[n] {
			t.Fatalf("next() returned duplicate nonce %d", n)
		}
		seen[n] = true
	}
}

func TestResolveNoWaiterIsNoop(t *testing.T) {
	r := NewResultRegistry()
	// Follower / replay path: resolve for a nonce nobody registered must not
	// panic or block.
	r.resolve(12345, ApplyResult{MsgID: 1})
	// And the reserved sentinel is always a no-op.
	r.resolve(0, ApplyResult{MsgID: 2})
}

func TestResolveIsSingleShot(t *testing.T) {
	r := NewResultRegistry()
	nonce, ch := r.Register()
	r.resolve(nonce, ApplyResult{MsgID: 7})
	<-ch
	// A second resolve for the same nonce (shouldn't happen, but must be
	// safe) is a no-op because register's entry was already deleted.
	r.resolve(nonce, ApplyResult{MsgID: 8})
	select {
	case v := <-ch:
		t.Fatalf("unexpected second value %+v", v)
	default:
	}
}

func TestRegistryConcurrent(t *testing.T) {
	r := NewResultRegistry()
	const n = 200
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(want uint64) {
			defer wg.Done()
			nonce, ch := r.Register()
			// Resolve from a second goroutine to exercise the store/load race.
			go r.resolve(nonce, ApplyResult{MsgID: want})
			if got := <-ch; got.MsgID != want {
				t.Errorf("nonce %d: got MsgID %d, want %d", nonce, got.MsgID, want)
			}
		}(uint64(i))
	}
	wg.Wait()
}
