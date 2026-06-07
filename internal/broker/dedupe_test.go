package broker

import "testing"

func TestDedupeLookupAfterInsert(t *testing.T) {
	d := NewDedupeIndex(4)
	d.Insert("a", 1)
	d.Insert("b", 2)

	if id, ok := d.Lookup("a"); !ok || id != 1 {
		t.Errorf("Lookup(a) = (%d, %v), want (1, true)", id, ok)
	}
	if id, ok := d.Lookup("b"); !ok || id != 2 {
		t.Errorf("Lookup(b) = (%d, %v), want (2, true)", id, ok)
	}
	if _, ok := d.Lookup("c"); ok {
		t.Errorf("Lookup(c) returned ok=true, want false")
	}
	if got := d.Len(); got != 2 {
		t.Errorf("Len = %d, want 2", got)
	}
}

func TestDedupeEvictsLRU(t *testing.T) {
	d := NewDedupeIndex(3)

	d.Insert("a", 1) // order (front→back): a
	d.Insert("b", 2) // order: b, a
	d.Insert("c", 3) // order: c, b, a
	d.Insert("d", 4) // over cap → a evicted; order: d, c, b

	if _, ok := d.Lookup("a"); ok {
		t.Errorf("expected a to be evicted")
	}
	for _, k := range []string{"b", "c", "d"} {
		if _, ok := d.Lookup(k); !ok {
			t.Errorf("expected %q to survive", k)
		}
	}
	if got := d.Len(); got != 3 {
		t.Errorf("Len = %d, want 3", got)
	}
}

func TestDedupeLookupBumpsToFront(t *testing.T) {
	d := NewDedupeIndex(3)

	d.Insert("a", 1)
	d.Insert("b", 2)
	d.Insert("c", 3)

	// Touch "a" — should now be MRU. Without this, "a" would be LRU
	// and the next insert would evict it.
	if _, ok := d.Lookup("a"); !ok {
		t.Fatalf("Lookup(a) should hit")
	}

	d.Insert("d", 4) // over cap → "b" should evict (now the LRU)

	if _, ok := d.Lookup("b"); ok {
		t.Errorf("expected b to be evicted (it was LRU after the lookup of a)")
	}
	if _, ok := d.Lookup("a"); !ok {
		t.Errorf("a should still be present — Lookup should have bumped it")
	}
}

func TestDedupeReinsertUpdatesValue(t *testing.T) {
	d := NewDedupeIndex(4)

	d.Insert("a", 1)
	d.Insert("a", 99) // same key, different msg_id

	if id, ok := d.Lookup("a"); !ok || id != 99 {
		t.Errorf("Lookup(a) = (%d, %v), want (99, true)", id, ok)
	}
	if got := d.Len(); got != 1 {
		t.Errorf("Len = %d, want 1 — re-insert should not grow the index", got)
	}
}
