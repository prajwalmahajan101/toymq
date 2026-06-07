package proto

type Command interface {
	isCommand()
}

type PubCommand struct {
	Topic     string
	DedupeKey string
	Payload   []byte
}

type SubCommand struct {
	Topic      string
	ConsumerID string
}

type AckCommand struct {
	ConsumerID string
	MsgID      uint64
}

type NackCommand struct {
	ConsumerID string
	MsgID      uint64
}

func (PubCommand) isCommand()  {}
func (SubCommand) isCommand()  {}
func (AckCommand) isCommand()  {}
func (NackCommand) isCommand() {}
