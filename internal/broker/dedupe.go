package broker

import "container/list"

// DedupeEntry is one (dedupe key, MsgID) pair stored in DedupeIndex.
// Exported so list.Element values type-assert cleanly.
type DedupeEntry struct {
	Key   string
	MsgID uint64
}

// DedupeIndex is a bounded LRU mapping producer dedupe keys to the
// MsgID of the original publish. Not goroutine-safe; the owning Topic
// serializes access via pubMu.
type DedupeIndex struct {
	cap   int
	order *list.List
	byKey map[string]*list.Element
}

// NewDedupeIndex returns an empty DedupeIndex bounded at capacity entries.
func NewDedupeIndex(capacity int) *DedupeIndex {
	return &DedupeIndex{
		cap:   capacity,
		order: list.New(),
		byKey: make(map[string]*list.Element),
	}
}

// Lookup returns the previously-stored MsgID for key and bumps the
// entry to most-recently-used. Returns (0, false) on miss.
func (d *DedupeIndex) Lookup(key string) (uint64, bool) {
	elem, ok := d.byKey[key]
	if !ok {
		return 0, false
	}
	d.order.MoveToFront(elem)
	return elem.Value.(DedupeEntry).MsgID, true
}

// Insert stores key→msgID, bumping the entry to MRU on overwrite and
// evicting the LRU entry when over capacity.
func (d *DedupeIndex) Insert(key string, msgID uint64) {
	if elem, ok := d.byKey[key]; ok {
		elem.Value = DedupeEntry{Key: key, MsgID: msgID}
		d.order.MoveToFront(elem)
		return
	}

	elem := d.order.PushFront(DedupeEntry{Key: key, MsgID: msgID})
	d.byKey[key] = elem

	if d.order.Len() > d.cap {
		oldest := d.order.Back()
		if oldest != nil {
			entry := oldest.Value.(DedupeEntry)
			delete(d.byKey, entry.Key)
			d.order.Remove(oldest)
		}
	}
}

// Len returns the current entry count.
func (d *DedupeIndex) Len() int {
	return d.order.Len()
}
