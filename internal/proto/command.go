package proto

// Command is the sealed union of wire verbs the parser can produce.
// The unexported isCommand marker keeps the union closed inside this
// package; see ADR 0004.
type Command interface {
	isCommand()
}

// PubCommand is a parsed PUB frame. Partition/PartitionSet carry an
// explicit target from PUB <topic>#<n>; otherwise RoutingKey (a field
// distinct from DedupeKey) selects the partition by hash, and an empty
// RoutingKey round-robins (ADR 0021).
type PubCommand struct {
	Topic        string
	DedupeKey    string
	RoutingKey   string
	Partition    int
	PartitionSet bool
	Payload      []byte

	// DelayMs holds the optional trailing DELAY <ms> token: the message is
	// held from delivery for this many milliseconds (ADR 0025). 0 (the
	// default, token absent) delivers immediately.
	DelayMs uint64
}

// SubCommand is a parsed SUB frame. AllPartitions is set by SUB <topic>
// (no suffix) or SUB <topic>#*; otherwise Partition is the single target
// from SUB <topic>#<n> (ADR 0021).
type SubCommand struct {
	Topic         string
	Partition     int
	AllPartitions bool
	ConsumerID    string
}

// AckCommand is a parsed ACK frame. Partition identifies which
// partition's log the MsgID belongs to (MsgIDs are partition-local).
type AckCommand struct {
	ConsumerID string
	Partition  int
	MsgID      uint64
}

// NackCommand is a parsed NACK frame.
type NackCommand struct {
	ConsumerID string
	Partition  int
	MsgID      uint64
}

// CreateCommand is a parsed CREATE frame: CREATE <topic> PARTITIONS <n>
// (ADR 0021). It is a member of the sealed Command union (ADR 0004).
type CreateCommand struct {
	Topic      string
	Partitions int
}

// PauseCommand and ResumeCommand are the argument-less flow-control frames
// (ADR 0022). They are session-scoped: they suspend / resume delivery for
// the connection's current subscription (every partition of a SUB #*),
// independent of the automatic receive window. Members of the sealed
// Command union (ADR 0004).
type PauseCommand struct{}

// ResumeCommand lifts a prior PAUSE for the connection's subscription.
type ResumeCommand struct{}

// TraceparentCommand is an optional, additive W3C trace-context prefix
// line (ADR 0026): TRACEPARENT <traceparent> [TRACESTATE <tracestate>].
// It carries no message payload; the session stashes it and applies the
// extracted remote parent to the NEXT PUB or SUB frame, then clears it.
// A connection that never sends it behaves exactly as pre-M7 — the
// opt-in contract. Member of the sealed Command union (ADR 0004).
type TraceparentCommand struct {
	Traceparent string
	Tracestate  string
}

func (PubCommand) isCommand()         {}
func (SubCommand) isCommand()         {}
func (AckCommand) isCommand()         {}
func (NackCommand) isCommand()        {}
func (CreateCommand) isCommand()      {}
func (PauseCommand) isCommand()       {}
func (ResumeCommand) isCommand()      {}
func (TraceparentCommand) isCommand() {}

// Hello is the parsed HELLO handshake frame. It is deliberately NOT a
// member of the Command union: HELLO is a one-shot handshake phase that
// precedes the steady-state command loop, not a verb the loop dispatches
// (ADR 0020). Version is the client's max supported wire version; Token
// is the optional AUTH bearer token ("" when absent).
type Hello struct {
	Version int
	Token   string
}
