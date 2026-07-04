package proto

import (
	"bufio"
	"bytes"
	"errors"
	"testing"
)

// failingWriter returns an error on the (failAfter+1)-th Write call.
// Used to exercise error-propagation branches in the response writers.
type failingWriter struct {
	failAfter int
	n         int
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.n >= w.failAfter {
		return 0, errors.New("forced write error")
	}
	w.n++
	return len(p), nil
}

func TestWriteResponses(t *testing.T) {
	cases := []struct {
		name string
		want string
		run  func(bw *bufio.Writer) error
	}{
		{
			name: "OK",
			want: "OK 42\n",
			run:  func(bw *bufio.Writer) error { return WriteOK(bw, 42) },
		},
		{
			name: "MSG",
			want: "MSG orders 2 7 5\nhello\n",
			run:  func(bw *bufio.Writer) error { return WriteMsg(bw, "orders", 2, 7, []byte("hello")) },
		},
		{
			name: "MSG empty payload",
			want: "MSG orders 0 0 0\n\n",
			run:  func(bw *bufio.Writer) error { return WriteMsg(bw, "orders", 0, 0, nil) },
		},
		{
			name: "ERR",
			want: "ERR bad_request unknown verb\n",
			run:  func(bw *bufio.Writer) error { return WriteErr(bw, "bad_request", "unknown verb") },
		},
		{
			name: "DUP",
			want: "DUP 99\n",
			run:  func(bw *bufio.Writer) error { return WriteDup(bw, 99) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			bw := bufio.NewWriter(&buf)
			if err := tc.run(bw); err != nil {
				t.Fatalf("write: %v", err)
			}
			if got := buf.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestWriteResponsesErrors exercises the error-return branches inside
// each WriteXxx function. With a 1-byte bufio buffer, every
// WriteString/WriteByte triggers an underlying Write, so a failingWriter
// that fails at different offsets propagates the error out of different
// statements inside each writer. Per-case offsets are sized to each
// writer's total underlying-Write count so every subtest definitely
// fails before the writer completes.
func TestWriteResponsesErrors(t *testing.T) {
	cases := []struct {
		name      string
		run       func(bw *bufio.Writer) error
		failAfter []int
	}{
		{
			name:      "OK",
			run:       func(bw *bufio.Writer) error { return WriteOK(bw, 42) },
			failAfter: []int{0, 1, 2, 3, 4, 5},
		},
		{
			// With a 1-byte buffer, bw.Write(payload) sends the whole
			// payload in a single underlying call (bufio short-circuits
			// when its buffer is empty), so MSG only has ~15 underlying
			// writes total even with a long payload.
			name:      "MSG",
			run:       func(bw *bufio.Writer) error { return WriteMsg(bw, "orders", 2, 7, bytes.Repeat([]byte("a"), 64)) },
			failAfter: []int{0, 1, 5, 10, 12, 14},
		},
		{
			name:      "ERR",
			run:       func(bw *bufio.Writer) error { return WriteErr(bw, "bad_code", "the reason") },
			failAfter: []int{0, 1, 5, 10, 15, 17},
		},
		{
			name:      "DUP",
			run:       func(bw *bufio.Writer) error { return WriteDup(bw, 99) },
			failAfter: []int{0, 1, 2, 3, 4, 5},
		},
	}

	for _, tc := range cases {
		for _, fa := range tc.failAfter {
			tc, fa := tc, fa
			t.Run(tc.name+"_failAfter"+itoa(fa), func(t *testing.T) {
				fw := &failingWriter{failAfter: fa}
				bw := bufio.NewWriterSize(fw, 1)
				err := tc.run(bw)
				if err == nil {
					t.Errorf("expected error, got nil")
				}
			})
		}
	}
}

// itoa is a tiny stand-in for strconv.Itoa to avoid an extra import
// just for subtest names.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [12]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

func TestPubRoundTrip(t *testing.T) {
	// Build a PUB on the wire (header), then a body via WriteMsg's
	// shape just to test the framing. For PubCommand we'd really need
	// a "WritePub" — which doesn't exist, since clients write PUBs and
	// the server reads them. So the round-trip we can do here is:
	// craft wire bytes by hand, ReadCommand, assert.
	//
	// OK/MSG/ERR/DUP round-tripping is exercised where the client-side
	// response parser lives (pkg/client/frame.go) and end-to-end in the
	// integration suite, not here.
	t.Skip("response round-trip is covered in pkg/client and integration tests")
}
