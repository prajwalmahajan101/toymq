package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"

	"github.com/prajwalmahajan101/toymq/internal/broker"
	"github.com/prajwalmahajan101/toymq/internal/proto"
)

const (
	defaultMaxPayload = 1 << 20 // 1MiB
	respChBuf         = 8
	sendChBuf         = 64
)

// Session is the per-connection state machine: one reader goroutine
// parsing commands, one writer goroutine serializing OK/MSG/DUP/ERR
// frames, and the two channels they share. See ADR 0008.
type Session struct {
	conn       io.ReadWriteCloser
	broker     *broker.Broker
	maxPayload int
	auth       authConfig

	respCh chan func(*bufio.Writer) error
	sendCh chan *broker.Inflight

	quit       chan struct{} // closed by Run to ask writer to exit
	writerDone chan struct{} // closed by writer on its way out

	// reader-only state - only the reader goroutine touches these.
	// currentSubs holds one Subscription per partition (a single entry
	// for a partition-scoped SUB, N for an all-partitions SUB); a single
	// currentCancel cancels all of them (ADR 0021).
	currentTopic  string
	currentSubs   []*broker.Subscription
	currentCancel context.CancelFunc
}

// NewSession builds an idle Session ready for Run. The underlying
// conn is closed by Run on exit. The zero-value authConfig disables the
// handshake (pre-M3 behaviour).
func NewSession(conn io.ReadWriteCloser, b *broker.Broker, auth authConfig) *Session {
	return &Session{
		conn:       conn,
		broker:     b,
		maxPayload: defaultMaxPayload,
		auth:       auth,
		respCh:     make(chan func(*bufio.Writer) error, respChBuf),
		sendCh:     make(chan *broker.Inflight, sendChBuf),
		quit:       make(chan struct{}),
		writerDone: make(chan struct{}),
	}
}

// Run starts the writer goroutine, runs the reader inline, then
// tears down the subscription (if any), signals the writer to exit,
// waits, and closes the conn. Blocks until the connection closes
// or ctx cancels.
func (s *Session) Run(ctx context.Context) {
	remote := remoteAddr(s.conn)
	slog.Debug("session opened", "remote-addr", remote)

	br := bufio.NewReader(s.conn)
	bw := bufio.NewWriter(s.conn)

	// Handshake is a synchronous request/response phase written directly
	// to bw — it runs BEFORE the async writer goroutine starts, so a
	// rejection's ERR frame can never be dropped by teardown racing the
	// respCh (ADR 0020).
	compatLine, ok := s.handshake(br, bw)
	if !ok {
		_ = s.conn.Close()
		slog.Debug("session closed", "remote-addr", remote, "reason", "handshake-rejected")
		return
	}

	go s.runWriter()

	s.runReader(ctx, br, compatLine)

	if s.currentCancel != nil {
		s.currentCancel()
	}
	close(s.quit)
	<-s.writerDone
	_ = s.conn.Close()
	slog.Debug("session closed", "remote-addr", remote)
}

// remoteAddr extracts the conn's remote address when it's a real
// net.Conn; tests pass io.ReadWriteCloser wrappers (e.g. net.Pipe
// halves) that don't expose one — fall back to "unknown" so logging
// stays useful without panicking.
func remoteAddr(conn io.ReadWriteCloser) string {
	if c, ok := conn.(net.Conn); ok {
		return c.RemoteAddr().String()
	}
	return "unknown"
}

func (s *Session) runReader(ctx context.Context, br *bufio.Reader, compatLine string) {
	// In compat mode the handshake handed back a non-HELLO first line to
	// process as a command before the steady-state loop.
	if compatLine != "" {
		if !s.processLine(ctx, compatLine, br) {
			return
		}
	}

	for {
		cmd, err := proto.ReadCommand(br, s.maxPayload)
		if !s.handleParsed(ctx, cmd, err) {
			return
		}
	}
}

// handshake runs the HELLO exchange (ADR 0020), writing its response
// synchronously to bw. It returns (compatLine, proceed): proceed=false
// means close the connection; a non-empty compatLine is the first line
// to process as a command when --require-hello is off and the client
// skipped HELLO.
func (s *Session) handshake(br *bufio.Reader, bw *bufio.Writer) (string, bool) {
	h, line, err := proto.ReadHello(br)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return "", false // client hung up before handshaking
		}
		if errors.Is(err, proto.ErrNotHello) {
			if s.auth.requireHello {
				_ = proto.WriteErr(bw, proto.ErrCodeHello, "expected HELLO as first frame")
				return "", false
			}
			return line, true // compat: process this line as a command
		}
		// Malformed HELLO or bad framing.
		_ = proto.WriteErr(bw, proto.ErrCodeHello, err.Error())
		return "", false
	}

	if s.auth.authEnabled() && !s.auth.checkToken(h.Token) {
		_ = proto.WriteErr(bw, proto.ErrCodeAuth, "invalid or missing token")
		return "", false
	}

	negotiated := min(h.Version, serverMaxVersion)
	if err := proto.WriteHelloOK(bw, negotiated); err != nil {
		return "", false
	}
	return "", true
}

// processLine handles one already-read header line as a command (compat
// first-line path). Returns false to close the connection.
func (s *Session) processLine(ctx context.Context, line string, br *bufio.Reader) bool {
	cmd, err := proto.ParseCommandLine(line, br, s.maxPayload)
	return s.handleParsed(ctx, cmd, err)
}

// handleParsed applies the shared post-parse policy: EOF/framing errors
// close the connection, INVALID/oversized get an ERR and continue, and a
// good command is dispatched. Returns false to stop the reader.
func (s *Session) handleParsed(ctx context.Context, cmd proto.Command, err error) bool {
	if err != nil {
		if errors.Is(err, io.EOF) {
			return false
		}
		if errors.Is(err, proto.ErrInvalidCommand) || errors.Is(err, proto.ErrPayloadTooLarge) {
			reason := err.Error()
			s.sendResp(func(bw *bufio.Writer) error {
				return proto.WriteErr(bw, "INVALID", reason)
			})
			return true
		}
		// ErrBadFraming or I/O - stream desynced, give up.
		return false
	}

	select {
	case <-ctx.Done():
		return false
	default:
	}
	s.handleCommand(ctx, cmd)
	return true
}

func (s *Session) sendResp(fn func(*bufio.Writer) error) {
	select {
	case s.respCh <- fn:
	case <-s.writerDone:
	}
}

func (s *Session) handleCommand(ctx context.Context, cmd proto.Command) {
	switch c := cmd.(type) {
	case proto.PubCommand:
		s.handlePub(c)
	case proto.AckCommand:
		s.handleAck(c)
	case proto.NackCommand:
		s.handleNack(c)
	case proto.SubCommand:
		s.handleSub(ctx, c)
	case proto.CreateCommand:
		s.handleCreate(c)
	case proto.PauseCommand:
		s.handlePauseResume(true)
	case proto.ResumeCommand:
		s.handlePauseResume(false)
	}
}

// handlePauseResume applies a PAUSE (paused=true) or RESUME to every
// partition of the connection's current subscription (ADR 0022). Without a
// prior SUB there is nothing to gate, so it is a NO_SUB error.
func (s *Session) handlePauseResume(paused bool) {
	if len(s.currentSubs) == 0 {
		verb := "RESUME"
		if paused {
			verb = "PAUSE"
		}
		s.sendResp(func(bw *bufio.Writer) error {
			return proto.WriteErr(bw, "NO_SUB", verb+" requires a prior SUB")
		})
		return
	}
	for _, sub := range s.currentSubs {
		sub.SetPaused(paused)
	}
	s.sendResp(func(bw *bufio.Writer) error {
		return proto.WriteOK(bw, 0)
	})
}

func (s *Session) handlePub(c proto.PubCommand) {
	id, _, dup, err := s.broker.Publish(c.Topic, c.DedupeKey, c.RoutingKey, c.Partition, c.PartitionSet, c.Payload)
	if err != nil {
		reason := err.Error()
		s.sendResp(func(bw *bufio.Writer) error {
			return proto.WriteErr(bw, "PUB_FAILED", reason)
		})
		return
	}
	if dup {
		// A duplicate replies DUP <original-id> then OK <original-id>, so
		// callers that only read one response frame still see the OK.
		s.sendResp(func(bw *bufio.Writer) error {
			return proto.WriteDup(bw, id)
		})
	}
	s.sendResp(func(bw *bufio.Writer) error {
		return proto.WriteOK(bw, id)
	})
}

func (s *Session) handleCreate(c proto.CreateCommand) {
	if err := s.broker.CreateTopic(c.Topic, c.Partitions); err != nil {
		reason := err.Error()
		s.sendResp(func(bw *bufio.Writer) error {
			return proto.WriteErr(bw, "CREATE_FAILED", reason)
		})
		return
	}
	s.sendResp(func(bw *bufio.Writer) error {
		return proto.WriteOK(bw, 0)
	})
}

func (s *Session) handleAck(c proto.AckCommand) {
	if s.currentTopic == "" {
		s.sendResp(func(bw *bufio.Writer) error {
			return proto.WriteErr(bw, "NO_SUB", "ACK requires a prior SUB")
		})
		return
	}
	if err := s.broker.Ack(s.currentTopic, c.Partition, c.ConsumerID, c.MsgID); err != nil {
		reason := err.Error()
		s.sendResp(func(bw *bufio.Writer) error {
			return proto.WriteErr(bw, "ACK_FAILED", reason)
		})
		return
	}
	msgID := c.MsgID

	s.sendResp(func(bw *bufio.Writer) error {
		return proto.WriteOK(bw, msgID)
	})
}

func (s *Session) handleNack(c proto.NackCommand) {
	if s.currentTopic == "" {
		s.sendResp(func(bw *bufio.Writer) error {
			return proto.WriteErr(bw, "NO_SUB", "NACK requires a prior SUB")
		})
		return
	}
	if err := s.broker.Nack(s.currentTopic, c.Partition, c.ConsumerID, c.MsgID, s.sendCh); err != nil {
		reason := err.Error()
		s.sendResp(func(bw *bufio.Writer) error {
			return proto.WriteErr(bw, "NACK_FAILED", reason)
		})
		return
	}
	msgID := c.MsgID
	s.sendResp(func(bw *bufio.Writer) error {
		return proto.WriteOK(bw, msgID)
	})
}

func (s *Session) handleSub(ctx context.Context, c proto.SubCommand) {
	if s.currentCancel != nil {
		s.currentCancel()
		s.currentSubs = nil
		s.currentCancel = nil
	}

	// Ensure the topic and validate the partition selector BEFORE queuing
	// the SUB acknowledgement, so the OK is on respCh ahead of any MSG the
	// delivery goroutines push onto sendCh. Combined with the writer's
	// respCh priority, the OK deterministically precedes the first MSG.
	count, err := s.broker.TopicPartitions(c.Topic)
	if err != nil {
		reason := err.Error()
		s.sendResp(func(bw *bufio.Writer) error {
			return proto.WriteErr(bw, "SUB_FAILED", reason)
		})
		return
	}
	if !c.AllPartitions && (c.Partition < 0 || c.Partition >= count) {
		s.sendResp(func(bw *bufio.Writer) error {
			return proto.WriteErr(bw, "SUB_FAILED", "partition out of range")
		})
		return
	}
	s.sendResp(func(bw *bufio.Writer) error {
		return proto.WriteOK(bw, 0)
	})

	subCtx, cancel := context.WithCancel(ctx)
	subs, err := s.broker.Subscribe(subCtx, c.Topic, c.Partition, c.AllPartitions, c.ConsumerID, s.sendCh)
	if err != nil {
		// Unreachable in practice — the topic exists and the selector was
		// range-checked above. Tear down defensively without a second frame.
		cancel()
		return
	}

	s.currentSubs = subs
	s.currentCancel = cancel
	s.currentTopic = c.Topic
}

func (s *Session) runWriter() {
	defer close(s.writerDone)

	bw := bufio.NewWriter(s.conn)
	for {
		// Prioritise protocol responses (OK/DUP/ERR) over async MSG
		// pushes: drain any queued response before touching sendCh, so a
		// request's response always precedes the deliveries it triggered
		// (e.g. SUB's OK before the first MSG). Go's select is random when
		// multiple cases are ready, so this explicit two-tier select is
		// what enforces the ordering.
		select {
		case fn := <-s.respCh:
			if err := fn(bw); err != nil {
				_ = s.conn.Close()
				return
			}
			continue
		case <-s.quit:
			return
		default:
		}

		select {
		case fn := <-s.respCh:
			if err := fn(bw); err != nil {
				_ = s.conn.Close()
				return
			}
		case inf := <-s.sendCh:
			if err := proto.WriteMsg(bw, inf.Topic, inf.Partition, inf.MsgID, inf.Payload); err != nil {
				_ = s.conn.Close()
				return
			}
		case <-s.quit:
			return
		}
	}
}
