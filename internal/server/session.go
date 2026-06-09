package server

import (
	"bufio"
	"context"
	"errors"
	"io"

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

	respCh chan func(*bufio.Writer) error
	sendCh chan *broker.Inflight

	quit       chan struct{} // closed by Run to ask writer to exit
	writerDone chan struct{} // closed by writer on its way out

	// reader-onlu state - only the reader goroutine touches these
	currentTopic  string
	currentSub    *broker.Subscription
	currentCancel context.CancelFunc
}

// NewSession builds an idle Session ready for Run. The underlying
// conn is closed by Run on exit.
func NewSession(conn io.ReadWriteCloser, b *broker.Broker) *Session {
	return &Session{
		conn:       conn,
		broker:     b,
		maxPayload: defaultMaxPayload,
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
	go s.runWriter()

	s.runReader(ctx)

	if s.currentCancel != nil {
		s.currentCancel()
	}
	close(s.quit)
	<-s.writerDone
	_ = s.conn.Close()
}

func (s *Session) runReader(ctx context.Context) {
	br := bufio.NewReader(s.conn)
	for {
		cmd, err := proto.ReadCommand(br, s.maxPayload)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			if errors.Is(err, proto.ErrInvalidCommand) || errors.Is(err, proto.ErrPayloadTooLarge) {
				reason := err.Error()
				s.sendResp(func(bw *bufio.Writer) error {
					return proto.WriteErr(bw, "INVALID", reason)
				})
				continue
			}
			// ErrBadFraming or I/O - stream desyned, give up.
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
		s.handleCommand(ctx, cmd)
	}
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
	}
}

func (s *Session) handlePub(c proto.PubCommand) {
	id, dup, err := s.broker.Publish(c.Topic, c.DedupeKey, c.Payload)
	if err != nil {
		reason := err.Error()
		s.sendResp(func(bw *bufio.Writer) error {
			return proto.WriteErr(bw, "PUB_FAILED", reason)
		})
	}
	if dup {
		s.sendResp(func(bw *bufio.Writer) error {
			return proto.WriteDup(bw, id)
		})
	}
	s.sendResp(func(bw *bufio.Writer) error {
		return proto.WriteOK(bw, id)
	})
}

func (s *Session) handleAck(c proto.AckCommand) {
	if s.currentTopic == "" {
		s.sendResp(func(bw *bufio.Writer) error {
			return proto.WriteErr(bw, "NO_SUB", "ACK requires a prior SUB")
		})
		return
	}
	if err := s.broker.Ack(s.currentTopic, c.ConsumerID, c.MsgID); err != nil {
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
	if err := s.broker.Nack(s.currentTopic, c.ConsumerID, c.MsgID, s.sendCh); err != nil {
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
		s.currentSub = nil
		s.currentCancel = nil
	}

	subCtx, cancel := context.WithCancel(ctx)
	sub, err := s.broker.Subscribe(subCtx, c.Topic, c.ConsumerID, s.sendCh)
	if err != nil {
		cancel()
		reason := err.Error()
		s.sendResp(func(bw *bufio.Writer) error {
			return proto.WriteErr(bw, "SUB_FAILED", reason)
		})
		return
	}

	s.currentSub = sub
	s.currentCancel = cancel
	s.currentTopic = c.Topic

	s.sendResp(func(bw *bufio.Writer) error {
		return proto.WriteOK(bw, 0)
	})
}

func (s *Session) runWriter() {
	defer close(s.writerDone)

	bw := bufio.NewWriter(s.conn)
	for {
		select {
		case fn := <-s.respCh:
			if err := fn(bw); err != nil {
				_ = s.conn.Close()
				return
			}
		case inf := <-s.sendCh:
			if err := proto.WriteMsg(bw, inf.Topic, inf.MsgID, inf.Payload); err != nil {
				_ = s.conn.Close()
				return
			}
		case <-s.quit:
			return
		}
	}
}
