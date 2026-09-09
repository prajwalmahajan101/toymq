package replication

import (
	"bytes"
	"reflect"
	"testing"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		env  Envelope
	}{
		{"publish", Envelope{
			Kind: KindPublish, Nonce: 42, Topic: "orders", Partition: 3,
			DedupeKey: "dk-1", TsNs: 1_700_000_000_000_000_000,
			VisibleAtNs: 1_700_000_005_000_000_000, Payload: []byte("hello world"),
		}},
		{"ack", Envelope{
			Kind: KindAck, Nonce: 7, Topic: "orders", Partition: 0,
			ConsumerID: "c-9", MsgID: 128,
		}},
		{"nack", Envelope{
			Kind: KindNack, Nonce: 8, Topic: "orders", Partition: 1,
			ConsumerID: "c-9", MsgID: 129,
		}},
		{"create", Envelope{
			Kind: KindCreateTopic, Nonce: 0, Topic: "events", Partitions: 8,
		}},
		{"empty-strings-nil-payload", Envelope{Kind: KindPublish}},
		{"zero-len-payload", Envelope{Kind: KindPublish, Payload: []byte{}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Decode(Encode(tc.env))
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			// A nil payload round-trips to an empty (len 0) slice; compare by
			// content, not by nil-ness.
			if !bytes.Equal(got.Payload, tc.env.Payload) {
				t.Errorf("Payload = %q, want %q", got.Payload, tc.env.Payload)
			}
			got.Payload, tc.env.Payload = nil, nil
			if !reflect.DeepEqual(got, tc.env) {
				t.Errorf("round-trip mismatch\n got: %+v\nwant: %+v", got, tc.env)
			}
		})
	}
}

func TestDecodeRejectsBadVersion(t *testing.T) {
	b := Encode(Envelope{Kind: KindPublish, Topic: "t"})
	b[0] = 99
	if _, err := Decode(b); err != ErrBadVersion {
		t.Fatalf("err = %v, want ErrBadVersion", err)
	}
}

func TestDecodeRejectsBadKind(t *testing.T) {
	b := Encode(Envelope{Kind: KindPublish, Topic: "t"})
	b[1] = 200
	if _, err := Decode(b); err != ErrBadKind {
		t.Fatalf("err = %v, want ErrBadKind", err)
	}
}

func TestDecodeRejectsTruncation(t *testing.T) {
	b := Encode(Envelope{Kind: KindPublish, Topic: "orders", Payload: []byte("xyz")})
	// Every proper-prefix shorter than the whole frame must fail, never
	// silently return a partial Envelope.
	for n := range len(b) {
		if _, err := Decode(b[:n]); err == nil {
			t.Errorf("Decode(b[:%d]) succeeded, want error", n)
		}
	}
}

func TestDecodeRejectsTrailingGarbage(t *testing.T) {
	b := Encode(Envelope{Kind: KindAck, Topic: "t", ConsumerID: "c", MsgID: 1})
	b = append(b, 0x00)
	if _, err := Decode(b); err != ErrTrailingGarbage {
		t.Fatalf("err = %v, want ErrTrailingGarbage", err)
	}
}

func TestDecodeRejectsOverlongLengthPrefix(t *testing.T) {
	// A topic length field claiming more bytes than the buffer holds must be
	// ErrShortRead, not an out-of-range slice panic.
	b := Encode(Envelope{Kind: KindPublish, Topic: "orders"})
	// Topic length prefix sits at: version(1)+kind(1)+nonce(8) = offset 10.
	b[10] = 0xff
	b[11] = 0xff
	if _, err := Decode(b); err != ErrShortRead {
		t.Fatalf("err = %v, want ErrShortRead", err)
	}
}
