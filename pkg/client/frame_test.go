package client

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"
)

func parse(t *testing.T, wire string) frame {
	t.Helper()
	r := bufio.NewReader(strings.NewReader(wire))
	f, err := readFrame(r)
	if err != nil {
		t.Fatalf("readFrame(%q): %v", wire, err)
	}
	return f
}

func TestReadFrame_OK(t *testing.T) {
	f := parse(t, "OK 42\n")
	if f.kind != frameOK || f.okID != 42 {
		t.Fatalf("got %+v", f)
	}
}

func TestReadFrame_DUP(t *testing.T) {
	f := parse(t, "DUP 7\n")
	if f.kind != frameDup || f.dupID != 7 {
		t.Fatalf("got %+v", f)
	}
}

func TestReadFrame_ERR(t *testing.T) {
	f := parse(t, "ERR BAD_ARG missing topic\n")
	if f.kind != frameErr || f.errCode != "BAD_ARG" || f.errMsg != "missing topic" {
		t.Fatalf("got %+v", f)
	}
}

func TestReadFrame_MSG(t *testing.T) {
	f := parse(t, "MSG orders 2 9 5\nhello\n")
	if f.kind != frameMsg || f.msgTopic != "orders" || f.msgPartition != 2 || f.msgID != 9 || string(f.payload) != "hello" {
		t.Fatalf("got %+v payload=%q", f, f.payload)
	}
}

func TestReadFrame_EOF(t *testing.T) {
	r := bufio.NewReader(strings.NewReader(""))
	_, err := readFrame(r)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF, got %v", err)
	}
}

func TestReadFrame_GarbageLine(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("HELLO world\n"))
	if _, err := readFrame(r); err == nil {
		t.Fatal("want error on unknown verb")
	}
}
