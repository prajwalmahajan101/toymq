package client

import (
	"errors"
	"fmt"
	"testing"
)

func TestServerErrClassification(t *testing.T) {
	// NOTLEADER → typed *NotLeaderError carrying the hint.
	err := serverErr(frame{kind: frameErr, errCode: "NOTLEADER", errMsg: "n2"})
	var nl *NotLeaderError
	if !errors.As(err, &nl) {
		t.Fatalf("NOTLEADER frame did not produce *NotLeaderError, got %T: %v", err, err)
	}
	if nl.Hint != "n2" {
		t.Errorf("hint = %q, want n2", nl.Hint)
	}

	// TRANSPORT → ErrTransport sentinel.
	if err := serverErr(frame{kind: frameErr, errCode: "TRANSPORT", errMsg: "boom"}); !errors.Is(err, ErrTransport) {
		t.Errorf("TRANSPORT frame = %v, want ErrTransport", err)
	}

	// Anything else → ErrServer, and NOT a NotLeaderError.
	other := serverErr(frame{kind: frameErr, errCode: "PUB_FAILED", errMsg: "nope"})
	if !errors.Is(other, ErrServer) {
		t.Errorf("PUB_FAILED frame = %v, want ErrServer", other)
	}
	if errors.As(other, &nl) {
		t.Errorf("PUB_FAILED wrongly matched *NotLeaderError")
	}
}

func TestNotLeaderErrorWrappedMatches(t *testing.T) {
	base := &NotLeaderError{Hint: "n3"}
	wrapped := fmt.Errorf("cluster: redirect: %w", base)
	var nl *NotLeaderError
	if !errors.As(wrapped, &nl) || nl.Hint != "n3" {
		t.Fatalf("errors.As failed to unwrap NotLeaderError: %v", wrapped)
	}
}
