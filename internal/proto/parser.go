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
