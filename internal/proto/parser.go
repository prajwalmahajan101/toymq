package proto

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func ReadCommand(br *bufio.Reader, maxPayload int) (Command, error) {
	line, err := readLine(br)
	if err != nil {
		return nil, err
	}
	return ParseCommandLine(line, br, maxPayload)
}

// ParseCommandLine dispatches an already-read header line to its verb
// parser (reading any payload body from br). Split out of ReadCommand so
// the session handshake can peek the first line for HELLO and, in compat
// mode, still process a non-HELLO first line as a command without
// re-reading it. See ADR 0020.
func ParseCommandLine(line string, br *bufio.Reader, maxPayload int) (Command, error) {
	fields := strings.Fields(line)

	if len(fields) == 0 {
		return nil, ErrInvalidCommand
	}

	switch fields[0] {
	case "PUB":
		return parsePub(br, fields, maxPayload)
	case "SUB":
		return parseSub(fields)

	case "ACK":
		return parseAck(fields)

	case "NACK":
		return parseNack(fields)
	default:
		return nil, fmt.Errorf("%w: Unknown verb %q", ErrInvalidCommand, fields[0])
	}
}

// ReadHello reads one line and parses it as a HELLO handshake frame.
// Returns ErrNotHello (with the raw line) when the first token is not
// HELLO, so the caller can decide whether to reject (strict) or fall
// back to treating the line as a command (compat). See ADR 0020.
func ReadHello(br *bufio.Reader) (h Hello, line string, err error) {
	line, err = readLine(br)
	if err != nil {
		return Hello{}, "", err
	}
	h, err = ParseHello(line)
	return h, line, err
}

// ParseHello parses "HELLO <version> [AUTH <token>]". It returns
// ErrNotHello if the line is not a HELLO frame, and ErrInvalidCommand
// for a malformed HELLO (bad arity or non-numeric version).
func ParseHello(line string) (Hello, error) {
	fields := strings.Fields(line)
	if len(fields) == 0 || fields[0] != "HELLO" {
		return Hello{}, ErrNotHello
	}
	// HELLO <version>  |  HELLO <version> AUTH <token>
	if len(fields) != 2 && len(fields) != 4 {
		return Hello{}, fmt.Errorf("%w: HELLO expects <version> [AUTH <token>], got %d args", ErrInvalidCommand, len(fields)-1)
	}
	version, err := strconv.Atoi(fields[1])
	if err != nil || version < 1 {
		return Hello{}, fmt.Errorf("%w: HELLO version %q", ErrInvalidCommand, fields[1])
	}
	h := Hello{Version: version}
	if len(fields) == 4 {
		if fields[2] != "AUTH" {
			return Hello{}, fmt.Errorf("%w: HELLO third field must be AUTH, got %q", ErrInvalidCommand, fields[2])
		}
		h.Token = fields[3]
	}
	return h, nil
}

func readLine(br *bufio.Reader) (string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		if err == io.EOF && line == "" {
			return "", io.EOF
		}
		if err == io.EOF {
			return "", ErrBadFraming
		}
		return "", err
	}
	if len(line) > MaxLineLength {
		return "", ErrBadFraming
	}

	return strings.TrimRight(line, "\r\n"), nil
}

func parsePub(br *bufio.Reader, fields []string, maxPayload int) (Command, error) {
	if len(fields) != 4 {
		return nil, fmt.Errorf("%w: PUB expects 3 args, got %d", ErrInvalidCommand, len(fields)-1)
	}
	topic := fields[1]
	key := fields[2]

	if key == "-" {
		key = ""
	}

	payloadLen, err := strconv.ParseUint(fields[3], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: PUB payload_len: %v", ErrInvalidCommand, err)
	}
	if payloadLen > uint64(maxPayload) {
		return nil, ErrPayloadTooLarge
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(br, payload); err != nil {
		return nil, ErrShortBody
	}
	nl, err := br.ReadByte()
	if err != nil {
		return nil, ErrBadFraming
	}

	if nl != '\n' {
		return nil, ErrBadFraming
	}
	return PubCommand{Topic: topic, DedupeKey: key, Payload: payload}, nil
}

func parseSub(fields []string) (Command, error) {
	if len(fields) != 3 {
		return nil, fmt.Errorf("%w: SUB expect 2 args, got %d", ErrInvalidCommand, len(fields)-1)
	}

	return SubCommand{Topic: fields[1], ConsumerID: fields[2]}, nil
}

func parseAck(fields []string) (Command, error) {
	if len(fields) != 3 {
		return nil, fmt.Errorf("%w: ACK expect 2 args, got %d", ErrInvalidCommand, len(fields)-1)
	}
	id, err := strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: ACK msg_id: %w", ErrInvalidCommand, err)
	}
	return AckCommand{ConsumerID: fields[1], MsgID: id}, nil
}

func parseNack(fields []string) (Command, error) {
	if len(fields) != 3 {
		return nil, fmt.Errorf("%w: NACK expect 2 args, got %d", ErrInvalidCommand, len(fields)-1)
	}
	id, err := strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: NACK msg_id: %w", ErrInvalidCommand, err)
	}
	return NackCommand{ConsumerID: fields[1], MsgID: id}, nil
}
