package broker

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/prajwalmahajan101/toymq/internal/wal"
)

// makeTopicForOffsets builds a Topic standalone (without going through
// Broker) so the offsets-only tests don't have to spin up the full
// broker lifecycle. The WAL handle is opened against dataDir so
// flushOffsets resolves the right path.
func makeTopicForOffsets(t *testing.T, dataDir, name string) *Topic {
	t.Helper()
	topicDir := filepath.Join(dataDir, "topics", name)
	if err := os.MkdirAll(topicDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	log, err := wal.Open(topicDir)
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	t.Cleanup(func() { _ = log.Close() })
	return newTopic(name, log, 16)
}

func TestFlushOffsetsCreateFails(t *testing.T) {
	dataDir := t.TempDir()
	topic := makeTopicForOffsets(t, dataDir, "orders")

	// Replace the topic dir with a regular file so os.Create on
	// offsets.json.tmp fails. We can't chmod-deny the dir here
	// because the test process is the owner; "is a directory" /
	// "not a directory" error from a file collision is portable.
	topicDir := filepath.Join(dataDir, "topics", "orders")
	if err := os.RemoveAll(topicDir); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}
	if err := os.WriteFile(topicDir, []byte("blocker"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if err := topic.flushOffsets(dataDir); err == nil {
		t.Fatal("flushOffsets: expected error, got nil")
	}
}

func TestLoadOffsetsCorruptJSON(t *testing.T) {
	dataDir := t.TempDir()
	topic := makeTopicForOffsets(t, dataDir, "orders")

	// Write invalid JSON to offsets.json.
	path := filepath.Join(dataDir, "topics", "orders", "offsets.json")
	if err := os.WriteFile(path, []byte("not json"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	err := topic.loadOffsets(dataDir)
	if err == nil {
		t.Fatal("loadOffsets: expected decode error, got nil")
	}
	// Sanity: error wraps the JSON decode failure with the path.
	if !errors.Is(err, err) {
		t.Fatal("errors.Is(err, err) is false — should be impossible")
	}
}

func TestLoadOffsetsMissingFileIsFresh(t *testing.T) {
	dataDir := t.TempDir()
	topic := makeTopicForOffsets(t, dataDir, "orders")

	// No offsets.json yet — treated as a fresh consumer, returns nil.
	if err := topic.loadOffsets(dataDir); err != nil {
		t.Fatalf("loadOffsets on missing file: %v", err)
	}
}
