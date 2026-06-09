package client

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type frameKind int

const (
	frameUnkown frameKind = iota
	frameOK
	frameDup
	frameErr
	frameMsg
)

// frame is one parsed wire frame. Only the fields meaningful for its
// kind are populated.
type frame struct {
	kind     frameKind
	okID     uint64
	dupID    uint64
	errCode  string
	errMsg   string
	msgTopic string
	msgID    uint64
	payload  []byte
}

// readFrame consumes exactly one frame from r. Returns io.EOF if the
// peer hung up cleanly between frames.
func readFrame(r *bufio.Reader) (frame, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return frame{}, err
	}

	line = strings.TrimRight(line, "\r\n")

	switch {
	case strings.HasPrefix(line, "OK "):
		id, err := strconv.ParseUint(strings.TrimPrefix(line, "OK "), 10, 64)
		if err != nil {
			return frame{}, fmt.Errorf("parse OK id %q: %w", line, err)
		}
		return frame{kind: frameOK, okID: id}, nil

	case strings.HasPrefix(line, "DUP "):
		id, err := strconv.ParseUint(strings.TrimPrefix(line, "DUP "), 10, 64)
		if err != nil {
			return frame{}, fmt.Errorf("parse DUP id %q: %w", line, err)
		}
		return frame{kind: frameDup, dupID: id}, nil

	case strings.HasPrefix(line, "ERR "):
		rest := strings.TrimPrefix(line, "ERR ")
		parts := strings.SplitN(rest, " ", 2)
		if len(parts) != 2 {
			return frame{}, fmt.Errorf("malformed ERR %q", line)
		}
		return frame{kind: frameErr, errCode: parts[0], errMsg: parts[1]}, nil

	case strings.HasPrefix(line, "MSG "):
		fields := strings.Fields(line)
		if len(fields) != 4 {
			return frame{}, fmt.Errorf("bad MSG header %q", line)
		}
		id, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return frame{}, fmt.Errorf("parse MSG id %q: %w", fields[2], err)
		}
		plen, err := strconv.Atoi(fields[3])
		if err != nil || plen < 0 {
			return frame{}, fmt.Errorf("parse MSG len %q: %w", fields[3], err)
		}
		payload := make([]byte, plen)
		if _, err := io.ReadFull(r, payload); err != nil {
			return frame{}, fmt.Errorf("read MSG payload: %w", err)
		}
		nl, err := r.ReadByte()
		if err != nil {
			return frame{}, fmt.Errorf("read MSG trailer: %w", err)
		}
		if nl != '\n' {
			return frame{}, errors.New("MSG trailer not newline")
		}
		return frame{kind: frameMsg, msgTopic: fields[1], msgID: id, payload: payload}, nil
	}

	return frame{}, fmt.Errorf("unknown frame %q", line)
}
