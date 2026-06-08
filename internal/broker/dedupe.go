package broker

import "container/list"

type DedupeEntry struct {
	Key   string
	MsgID uint64
}

type DedupeIndex struct {
	cap   int
	order *list.List
	byKey map[string]*list.Element
}

func NewDedupeIndex(cap int) *DedupeIndex {
	return &DedupeIndex{
		cap:   cap,
		order: list.New(),
		byKey: make(map[string]*list.Element),
	}
}

func (d *DedupeIndex) Lookup(key string) (uint64, bool) {
	elem, ok := d.byKey[key]
	if !ok {
		return 0, false
	}
	d.order.MoveToFront(elem)
	return elem.Value.(DedupeEntry).MsgID, true
}

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

func (d *DedupeIndex) Len() int {
	return d.order.Len()
}
