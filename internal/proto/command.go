package proto

// Command is the sealed union of wire verbs the parser can produce.
// The unexported isCommand marker keeps the union closed inside this
// package; see ADR 0004.
type Command interface {
	isCommand()
}

// PubCommand is a parsed PUB frame.
type PubCommand struct {
	Topic     string
	DedupeKey string
	Payload   []byte
}

// SubCommand is a parsed SUB frame.
type SubCommand struct {
	Topic      string
	ConsumerID string
}

// AckCommand is a parsed ACK frame.
type AckCommand struct {
	ConsumerID string
	MsgID      uint64
}

// NackCommand is a parsed NACK frame.
type NackCommand struct {
	ConsumerID string
	MsgID      uint64
}

func (PubCommand) isCommand()  {}
func (SubCommand) isCommand()  {}
func (AckCommand) isCommand()  {}
func (NackCommand) isCommand() {}
