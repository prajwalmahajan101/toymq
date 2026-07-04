package broker

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
)

type offsetsFile struct {
	Consumers map[string]consumerOffsets `json:"consumers"`
}

type consumerOffsets struct {
	HasAcked  bool     `json:"has_acked"`
	LastAcked uint64   `json:"last_acked"`
	AboveLast []uint64 `json:"above_last"`
}

func (p *Partition) snapshotOffsets() offsetsFile {
	p.consumersMu.RLock()
	defer p.consumersMu.RUnlock()

	out := offsetsFile{
		Consumers: make(map[string]consumerOffsets, len(p.consumers)),
	}

	for id, c := range p.consumers {
		c.mu.Lock()
		snap := consumerOffsets{
			HasAcked:  c.hasAcked,
			LastAcked: c.lastAcked,
			AboveLast: make([]uint64, 0, len(c.aboveLast)),
		}
		for k := range c.aboveLast {
			snap.AboveLast = append(snap.AboveLast, k)
		}
		c.mu.Unlock()
		slices.Sort(snap.AboveLast)
		out.Consumers[id] = snap
	}

	return out
}

// flushOffsets atomically writes this partition's consumer offsets to
// <dir>/offsets.json (dir is the partition's own directory — the topic
// dir for a 1-partition/flat topic, or topics/<name>/<id> for N>1).
func (p *Partition) flushOffsets() error {
	p.consumersMu.RLock()
	for _, c := range p.consumers {
		c.persistDirty.Store(false)
	}
	p.consumersMu.RUnlock()
	snapshot := p.snapshotOffsets()

	path := filepath.Join(p.dir, "offsets.json")
	tmp := path + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}

	if err := json.NewEncoder(f).Encode(snapshot); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("encode offsets: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename %s -> %s: %w", tmp, path, err)
	}
	slog.Debug("offsets flushed", "topic", p.topic, "partition", p.id)
	return nil
}

func (p *Partition) loadOffsets() error {
	path := filepath.Join(p.dir, "offsets.json")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open: %s: %w", path, err)
	}
	defer f.Close()

	var data offsetsFile

	if err := json.NewDecoder(f).Decode(&data); err != nil {
		return fmt.Errorf("decode: %s: %w", path, err)
	}

	for id, off := range data.Consumers {
		c := p.getOrCreateConsumer(id)
		c.mu.Lock()
		c.hasAcked = off.HasAcked
		c.lastAcked = off.LastAcked
		for _, mid := range off.AboveLast {
			c.aboveLast[mid] = struct{}{}
		}
		c.mu.Unlock()
	}

	return nil
}
