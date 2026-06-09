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
			name:  "PUB with key",
			input: "PUB orders order-123 5\nhello\n",
			want:  PubCommand{Topic: "orders", DedupeKey: "order-123", Payload: []byte("hello")},
		},
		{
			name:  "PUB no key (dash)",
			input: "PUB orders - 5\nhello\n",
			want:  PubCommand{Topic: "orders", DedupeKey: "", Payload: []byte("hello")},
		},
		{
			name:  "PUB empty payload",
			input: "PUB orders - 0\n\n",
			want:  PubCommand{Topic: "orders", DedupeKey: "", Payload: []byte{}},
		},
		{
			name:  "SUB",
			input: "SUB orders consumer-1\n",
			want:  SubCommand{Topic: "orders", ConsumerID: "consumer-1"},
		},
		{
			name:  "ACK",
			input: "ACK consumer-1 42\n",
			want:  AckCommand{ConsumerID: "consumer-1", MsgID: 42},
		},
		{
			name:  "NACK",
			input: "NACK consumer-1 42\n",
			want:  NackCommand{ConsumerID: "consumer-1", MsgID: 42},
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
		{"PUB bad len", "PUB orders - notanumber\n", ErrInvalidCommand},
		{"PUB short body", "PUB orders - 10\nhi\n", ErrShortBody},
		{"PUB no trailing newline", "PUB orders - 2\nhi", ErrBadFraming},
		{"SUB missing arg", "SUB orders\n", ErrInvalidCommand},
		{"ACK bad id", "ACK consumer-1 notanumber\n", ErrInvalidCommand},
		{"NACK missing arg", "NACK consumer-1\n", ErrInvalidCommand},
		{"empty line", "\n", ErrInvalidCommand},
		{"EOF mid-line", "PUB orders", ErrBadFraming},
		{"ACK missing arg", "ACK consumer-1\n", ErrInvalidCommand},
		{"ACK extra arg", "ACK c1 42 extra\n", ErrInvalidCommand},
		{"NACK extra arg", "NACK c1 42 extra\n", ErrInvalidCommand},
		{"NACK bad id", "NACK c1 notanumber\n", ErrInvalidCommand},
		{"SUB extra arg", "SUB t c1 extra\n", ErrInvalidCommand},
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
		br := bufio.NewReader(strings.NewReader("PUB orders - 100\n" + strings.Repeat("x", 100) + "\n"))
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
