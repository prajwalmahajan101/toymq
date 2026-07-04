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

func (PubCommand) isCommand()    {}
func (SubCommand) isCommand()    {}
func (AckCommand) isCommand()    {}
func (NackCommand) isCommand()   {}
func (CreateCommand) isCommand() {}

// Hello is the parsed HELLO handshake frame. It is deliberately NOT a
// member of the Command union: HELLO is a one-shot handshake phase that
// precedes the steady-state command loop, not a verb the loop dispatches
// (ADR 0020). Version is the client's max supported wire version; Token
// is the optional AUTH bearer token ("" when absent).
type Hello struct {
	Version int
	Token   string
}
