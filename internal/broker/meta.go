package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// topicMeta is the on-disk topic descriptor (ADR 0021). It is written
// only when a topic has more than one partition — a flat, single-partition
// topic has no meta.json and is byte-for-byte identical to the pre-M4
// layout, so old data dirs recover unchanged.
type topicMeta struct {
	Partitions int `json:"partitions"`
}

// topicPartitionCount reports the on-disk partition count for a topic
// directory. It returns (n, true) for an existing topic — n from
// meta.json when present, else 1 for a flat/legacy layout — and
// (0, false) when the topic directory does not exist.
func topicPartitionCount(topicDir string) (count int, exists bool, err error) {
	data, err := os.ReadFile(filepath.Join(topicDir, "meta.json"))
	if err == nil {
		var m topicMeta
		if err := json.Unmarshal(data, &m); err != nil {
			return 0, false, fmt.Errorf("parse meta.json in %s: %w", topicDir, err)
		}
		if m.Partitions < 1 {
			return 0, false, fmt.Errorf("meta.json in %s has invalid partitions %d", topicDir, m.Partitions)
		}
		return m.Partitions, true, nil
	}
	if !os.IsNotExist(err) {
		return 0, false, fmt.Errorf("read meta.json in %s: %w", topicDir, err)
	}

	// No meta.json: a flat/legacy 1-partition topic if the dir exists.
	info, statErr := os.Stat(topicDir)
	if statErr != nil {
		if os.IsNotExist(statErr) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("stat %s: %w", topicDir, statErr)
	}
	if !info.IsDir() {
		return 0, false, fmt.Errorf("%s is not a directory", topicDir)
	}
	return 1, true, nil
}

// writeTopicMeta creates topicDir and persists the partition count. Only
// called for N>1 topics (flat topics stay meta-less).
func writeTopicMeta(topicDir string, partitions int) error {
	if err := os.MkdirAll(topicDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", topicDir, err)
	}
	data, err := json.Marshal(topicMeta{Partitions: partitions})
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}
	path := filepath.Join(topicDir, "meta.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
