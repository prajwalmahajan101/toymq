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

	case "CREATE":
		return parseCreate(fields)

	case "PAUSE":
		return parsePause(fields)

	case "RESUME":
		return parseResume(fields)

	case "TRACEPARENT":
		return parseTraceparent(line)

	case "INFO":
		return parseInfo(fields)

	default:
		return nil, fmt.Errorf("%w: Unknown verb %q", ErrInvalidCommand, fields[0])
	}
}

// parseTopicPartition splits a wire topic token into its base topic and
// an optional partition selector (ADR 0021):
//
//	orders    -> ("orders", 0, star=false, explicit=false)
//	orders#3  -> ("orders", 3, star=false, explicit=true)
//	orders#*  -> ("orders", 0, star=true,  explicit=false)
//
// A malformed suffix (empty base, empty/negative/non-numeric partition,
// or more than one '#') is an error.
func parseTopicPartition(tok string) (topic string, partition int, star, explicit bool, err error) {
	base, suffix, hasHash := strings.Cut(tok, "#")
	if !hasHash {
		if tok == "" {
			return "", 0, false, false, fmt.Errorf("%w: empty topic", ErrInvalidCommand)
		}
		return tok, 0, false, false, nil
	}
	if base == "" {
		return "", 0, false, false, fmt.Errorf("%w: empty topic in %q", ErrInvalidCommand, tok)
	}
	if strings.ContainsRune(suffix, '#') {
		return "", 0, false, false, fmt.Errorf("%w: malformed partition suffix in %q", ErrInvalidCommand, tok)
	}
	if suffix == "*" {
		return base, 0, true, false, nil
	}
	n, convErr := strconv.Atoi(suffix)
	if convErr != nil || n < 0 {
		return "", 0, false, false, fmt.Errorf("%w: partition %q in %q", ErrInvalidCommand, suffix, tok)
	}
	return base, n, false, true, nil
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
	// PUB <topic> <dedupe-key> <routing-key> <payload_len> [DELAY <ms>] [WAIT <n> <timeout-ms>]
	// The 4 positional args are followed by any mix of the optional
	// DELAY <ms> (ADR 0025) and WAIT <n> <timeout-ms> (ADR 0033) tokens, in
	// any order. Without them the frame is the pre-M6 5-field shape.
	if len(fields) < 5 {
		return nil, fmt.Errorf("%w: PUB expects at least 4 args (topic dedupe-key routing-key payload_len), got %d", ErrInvalidCommand, len(fields)-1)
	}
	var (
		delayMs       uint64
		waitReplicas  int
		waitTimeoutMs uint64
	)
	for i := 5; i < len(fields); {
		switch fields[i] {
		case "DELAY":
			if i+1 >= len(fields) {
				return nil, fmt.Errorf("%w: PUB DELAY needs <ms>", ErrInvalidCommand)
			}
			d, err := strconv.ParseUint(fields[i+1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("%w: PUB DELAY ms: %v", ErrInvalidCommand, err)
			}
			delayMs = d
			i += 2
		case "WAIT":
			if i+2 >= len(fields) {
				return nil, fmt.Errorf("%w: PUB WAIT needs <n> <timeout-ms>", ErrInvalidCommand)
			}
			n, err := strconv.Atoi(fields[i+1])
			if err != nil || n < 0 {
				return nil, fmt.Errorf("%w: PUB WAIT n: %q", ErrInvalidCommand, fields[i+1])
			}
			t, err := strconv.ParseUint(fields[i+2], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("%w: PUB WAIT timeout-ms: %v", ErrInvalidCommand, err)
			}
			waitReplicas = n
			waitTimeoutMs = t
			i += 3
		default:
			return nil, fmt.Errorf("%w: PUB unknown trailing token %q (want DELAY or WAIT)", ErrInvalidCommand, fields[i])
		}
	}
	topic, partition, star, explicit, err := parseTopicPartition(fields[1])
	if err != nil {
		return nil, err
	}
	if star {
		return nil, fmt.Errorf("%w: PUB cannot target %q (all partitions)", ErrInvalidCommand, fields[1])
	}

	key := fields[2]
	if key == "-" {
		key = ""
	}
	routingKey := fields[3]
	if routingKey == "-" {
		routingKey = ""
	}

	payloadLen, err := strconv.ParseUint(fields[4], 10, 64)
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
	return PubCommand{
		Topic:         topic,
		DedupeKey:     key,
		RoutingKey:    routingKey,
		Partition:     partition,
		PartitionSet:  explicit,
		Payload:       payload,
		DelayMs:       delayMs,
		WaitReplicas:  waitReplicas,
		WaitTimeoutMs: waitTimeoutMs,
	}, nil
}

// parseInfo parses INFO [<section>] (ADR 0033). Section defaults to
// "replication"; only that section exists today, so anything else is
// rejected rather than silently returning an empty block.
func parseInfo(fields []string) (Command, error) {
	section := "replication"
	switch len(fields) {
	case 1:
	case 2:
		section = fields[1]
	default:
		return nil, fmt.Errorf("%w: INFO expects at most one section arg, got %d", ErrInvalidCommand, len(fields)-1)
	}
	if section != "replication" {
		return nil, fmt.Errorf("%w: INFO unknown section %q (want replication)", ErrInvalidCommand, section)
	}
	return InfoCommand{Section: section}, nil
}

func parseSub(fields []string) (Command, error) {
	// SUB <topic> <consumer> [STALE]: the optional 4th token opts into a
	// follower-local read (ADR 0032). Any other 4th token is rejected.
	stale := false
	if len(fields) == 4 {
		if fields[3] != "STALE" {
			return nil, fmt.Errorf("%w: SUB unexpected 4th arg %q (want STALE)", ErrInvalidCommand, fields[3])
		}
		stale = true
	} else if len(fields) != 3 {
		return nil, fmt.Errorf("%w: SUB expect 2 args, got %d", ErrInvalidCommand, len(fields)-1)
	}
	topic, partition, star, explicit, err := parseTopicPartition(fields[1])
	if err != nil {
		return nil, err
	}
	// SUB <topic> (no suffix) and SUB <topic>#* both mean all partitions.
	all := star || !explicit
	return SubCommand{Topic: topic, Partition: partition, AllPartitions: all, ConsumerID: fields[2], Stale: stale}, nil
}

func parseAck(fields []string) (Command, error) {
	if len(fields) != 4 {
		return nil, fmt.Errorf("%w: ACK expect 3 args (consumer-id partition msg_id), got %d", ErrInvalidCommand, len(fields)-1)
	}
	partition, err := strconv.Atoi(fields[2])
	if err != nil || partition < 0 {
		return nil, fmt.Errorf("%w: ACK partition %q", ErrInvalidCommand, fields[2])
	}
	id, err := strconv.ParseUint(fields[3], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: ACK msg_id: %w", ErrInvalidCommand, err)
	}
	return AckCommand{ConsumerID: fields[1], Partition: partition, MsgID: id}, nil
}

func parseNack(fields []string) (Command, error) {
	if len(fields) != 4 {
		return nil, fmt.Errorf("%w: NACK expect 3 args (consumer-id partition msg_id), got %d", ErrInvalidCommand, len(fields)-1)
	}
	partition, err := strconv.Atoi(fields[2])
	if err != nil || partition < 0 {
		return nil, fmt.Errorf("%w: NACK partition %q", ErrInvalidCommand, fields[2])
	}
	id, err := strconv.ParseUint(fields[3], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: NACK msg_id: %w", ErrInvalidCommand, err)
	}
	return NackCommand{ConsumerID: fields[1], Partition: partition, MsgID: id}, nil
}

// parsePause / parseResume accept the bare verb only (ADR 0022): the
// frame carries no arguments because it is scoped to the connection's
// current subscription, not a specific topic/partition/consumer.
func parsePause(fields []string) (Command, error) {
	if len(fields) != 1 {
		return nil, fmt.Errorf("%w: PAUSE takes no arguments, got %d", ErrInvalidCommand, len(fields)-1)
	}
	return PauseCommand{}, nil
}

func parseResume(fields []string) (Command, error) {
	if len(fields) != 1 {
		return nil, fmt.Errorf("%w: RESUME takes no arguments, got %d", ErrInvalidCommand, len(fields)-1)
	}
	return ResumeCommand{}, nil
}

// parseTraceparent parses the optional trace-context prefix line (ADR 0026):
//
//	TRACEPARENT <traceparent>
//	TRACEPARENT <traceparent> TRACESTATE <tracestate>
//
// Both values are single space-free tokens (W3C traceparent is fixed-form;
// tracestate is a comma-separated list with no required spaces). Validation
// of the traceparent value itself is left to the W3C propagator downstream:
// a malformed value simply fails to extract a parent, degrading to a fresh
// root span rather than erroring the connection.
func parseTraceparent(line string) (Command, error) {
	fields := strings.Fields(line)
	switch len(fields) {
	case 2:
		return TraceparentCommand{Traceparent: fields[1]}, nil
	case 4:
		if fields[2] != "TRACESTATE" {
			return nil, fmt.Errorf("%w: TRACEPARENT third field must be TRACESTATE, got %q", ErrInvalidCommand, fields[2])
		}
		return TraceparentCommand{Traceparent: fields[1], Tracestate: fields[3]}, nil
	default:
		return nil, fmt.Errorf("%w: TRACEPARENT expects <traceparent> [TRACESTATE <tracestate>], got %d args", ErrInvalidCommand, len(fields)-1)
	}
}

func parseCreate(fields []string) (Command, error) {
	if len(fields) != 4 || fields[2] != "PARTITIONS" {
		return nil, fmt.Errorf("%w: CREATE expects <topic> PARTITIONS <n>", ErrInvalidCommand)
	}
	n, err := strconv.Atoi(fields[3])
	if err != nil || n < 1 {
		return nil, fmt.Errorf("%w: CREATE partitions %q", ErrInvalidCommand, fields[3])
	}
	return CreateCommand{Topic: fields[1], Partitions: n}, nil
}
