//go:build chaos

package chaos

import (
	"bytes"
	"sync"
)

// syncBuffer is a goroutine-safe wrapper around bytes.Buffer. Used
// as the shared stderr sink for the broker subprocess, the chaos
// producer, and the chaos consumer — all three write concurrently
// once redelivery surfaces ack errors, and -race trips on the
// underlying bytes.Buffer otherwise.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func (b *syncBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]byte, b.buf.Len())
	copy(out, b.buf.Bytes())
	return out
}
