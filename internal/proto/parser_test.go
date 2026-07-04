package proto

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

const testMaxPayload = 4 << 20

func TestReadCommandHappy(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  Command
	}{
		{
			name:  "PUB with keys",
			input: "PUB orders order-123 user-9 5\nhello\n",
			want:  PubCommand{Topic: "orders", DedupeKey: "order-123", RoutingKey: "user-9", Payload: []byte("hello")},
		},
		{
			name:  "PUB no keys (dash)",
			input: "PUB orders - - 5\nhello\n",
			want:  PubCommand{Topic: "orders", DedupeKey: "", RoutingKey: "", Payload: []byte("hello")},
		},
		{
			name:  "PUB explicit partition",
			input: "PUB orders#3 - - 5\nhello\n",
			want:  PubCommand{Topic: "orders", Partition: 3, PartitionSet: true, Payload: []byte("hello")},
		},
		{
			name:  "PUB empty payload",
			input: "PUB orders - - 0\n\n",
			want:  PubCommand{Topic: "orders", DedupeKey: "", Payload: []byte{}},
		},
		{
			name:  "SUB all partitions",
			input: "SUB orders consumer-1\n",
			want:  SubCommand{Topic: "orders", AllPartitions: true, ConsumerID: "consumer-1"},
		},
		{
			name:  "SUB star",
			input: "SUB orders#* consumer-1\n",
			want:  SubCommand{Topic: "orders", AllPartitions: true, ConsumerID: "consumer-1"},
		},
		{
			name:  "SUB single partition",
			input: "SUB orders#2 consumer-1\n",
			want:  SubCommand{Topic: "orders", Partition: 2, AllPartitions: false, ConsumerID: "consumer-1"},
		},
		{
			name:  "ACK",
			input: "ACK consumer-1 0 42\n",
			want:  AckCommand{ConsumerID: "consumer-1", Partition: 0, MsgID: 42},
		},
		{
			name:  "NACK",
			input: "NACK consumer-1 1 42\n",
			want:  NackCommand{ConsumerID: "consumer-1", Partition: 1, MsgID: 42},
		},
		{
			name:  "CREATE",
			input: "CREATE orders PARTITIONS 4\n",
			want:  CreateCommand{Topic: "orders", Partitions: 4},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			br := bufio.NewReader(strings.NewReader(tc.input))
			got, err := ReadCommand(br, testMaxPayload)
			if err != nil {
				t.Fatalf("ReadCommand: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestReadCommandErrors(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantErr error
	}{
		{"unknown verb", "FOO bar\n", ErrInvalidCommand},
		{"PUB missing args", "PUB orders\n", ErrInvalidCommand},
		{"PUB too few args", "PUB orders - 5\nhello\n", ErrInvalidCommand},
		{"PUB bad len", "PUB orders - - notanumber\n", ErrInvalidCommand},
		{"PUB short body", "PUB orders - - 10\nhi\n", ErrShortBody},
		{"PUB no trailing newline", "PUB orders - - 2\nhi", ErrBadFraming},
		{"PUB star partition", "PUB orders#* - - 5\nhello\n", ErrInvalidCommand},
		{"PUB bad partition", "PUB orders#x - - 5\nhello\n", ErrInvalidCommand},
		{"SUB missing arg", "SUB orders\n", ErrInvalidCommand},
		{"SUB bad partition", "SUB orders#x c1\n", ErrInvalidCommand},
		{"ACK bad id", "ACK consumer-1 0 notanumber\n", ErrInvalidCommand},
		{"ACK bad partition", "ACK consumer-1 x 42\n", ErrInvalidCommand},
		{"NACK missing arg", "NACK consumer-1\n", ErrInvalidCommand},
		{"empty line", "\n", ErrInvalidCommand},
		{"EOF mid-line", "PUB orders", ErrBadFraming},
		{"ACK missing arg", "ACK consumer-1 0\n", ErrInvalidCommand},
		{"ACK extra arg", "ACK c1 0 42 extra\n", ErrInvalidCommand},
		{"NACK extra arg", "NACK c1 0 42 extra\n", ErrInvalidCommand},
		{"NACK bad id", "NACK c1 0 notanumber\n", ErrInvalidCommand},
		{"SUB extra arg", "SUB t c1 extra\n", ErrInvalidCommand},
		{"CREATE missing keyword", "CREATE orders 4\n", ErrInvalidCommand},
		{"CREATE bad count", "CREATE orders PARTITIONS 0\n", ErrInvalidCommand},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			br := bufio.NewReader(strings.NewReader(tc.input))
			_, err := ReadCommand(br, testMaxPayload)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}

	t.Run("PUB payload exceeds max", func(t *testing.T) {
		br := bufio.NewReader(strings.NewReader("PUB orders - - 100\n" + strings.Repeat("x", 100) + "\n"))
		_, err := ReadCommand(br, 50) // max=50, payload=100
		if !errors.Is(err, ErrPayloadTooLarge) {
			t.Errorf("err = %v, want ErrPayloadTooLarge", err)
		}
	})

	t.Run("line exceeds MaxLineLength", func(t *testing.T) {
		input := strings.Repeat("a", MaxLineLength+10) + "\n"
		br := bufio.NewReader(strings.NewReader(input))
		_, err := ReadCommand(br, testMaxPayload)
		if !errors.Is(err, ErrBadFraming) {
			t.Errorf("err = %v, want ErrBadFraming", err)
		}
	})
}

func TestReadCommandCleanEOF(t *testing.T) {
	br := bufio.NewReader(bytes.NewReader(nil))
	_, err := ReadCommand(br, testMaxPayload)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}

func TestParseHello(t *testing.T) {
	cases := []struct {
		name    string
		line    string
		want    Hello
		wantErr error // nil = success; sentinel to errors.Is against
	}{
		{"version only", "HELLO 1", Hello{Version: 1}, nil},
		{"version and auth", "HELLO 1 AUTH s3cret", Hello{Version: 1, Token: "s3cret"}, nil},
		{"higher version", "HELLO 2 AUTH t", Hello{Version: 2, Token: "t"}, nil},
		{"not hello", "PUB orders - 3", Hello{}, ErrNotHello},
		{"empty", "", Hello{}, ErrNotHello},
		{"bad arity 3", "HELLO 1 AUTH", Hello{}, ErrInvalidCommand},
		{"bad version", "HELLO x", Hello{}, ErrInvalidCommand},
		{"zero version", "HELLO 0", Hello{}, ErrInvalidCommand},
		{"third field not AUTH", "HELLO 1 FOO bar", Hello{}, ErrInvalidCommand},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseHello(tc.line)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want Is(%v)", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestReadHelloFallsBackToLine(t *testing.T) {
	// A non-HELLO first line returns ErrNotHello but hands back the raw
	// line so the caller can process it as a command in compat mode.
	br := bufio.NewReader(strings.NewReader("PUB orders - - 5\nhello\n"))
	_, line, err := ReadHello(br)
	if !errors.Is(err, ErrNotHello) {
		t.Fatalf("err = %v, want ErrNotHello", err)
	}
	cmd, err := ParseCommandLine(line, br, testMaxPayload)
	if err != nil {
		t.Fatalf("parseCommandLine on fallback: %v", err)
	}
	pub, ok := cmd.(PubCommand)
	if !ok || pub.Topic != "orders" || string(pub.Payload) != "hello" {
		t.Fatalf("fallback command = %#v", cmd)
	}
}

func TestWriteHelloOK(t *testing.T) {
	var buf bytes.Buffer
	bw := bufio.NewWriter(&buf)
	if err := WriteHelloOK(bw, 1); err != nil {
		t.Fatalf("WriteHelloOK: %v", err)
	}
	if got := buf.String(); got != "HELLO 1 OK\n" {
		t.Fatalf("wrote %q, want %q", got, "HELLO 1 OK\n")
	}
}
