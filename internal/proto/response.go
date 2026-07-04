package proto

import (
	"bufio"
	"strconv"
)

// WriteOK writes "OK <msgID>\n" and flushes. Used as the success
// response to PUB / ACK / NACK and the SUB acceptance frame
// (msgID=0).
func WriteOK(bw *bufio.Writer, msgID uint64) error {
	if _, err := bw.WriteString("OK "); err != nil {
		return err
	}
	if _, err := bw.WriteString(strconv.FormatUint(msgID, 10)); err != nil {
		return err
	}
	if err := bw.WriteByte('\n'); err != nil {
		return err
	}
	return bw.Flush()
}

// WriteHelloOK writes "HELLO <version> OK\n" and flushes — the server's
// accepting handshake response, echoing the negotiated wire version
// (ADR 0020).
func WriteHelloOK(bw *bufio.Writer, version int) error {
	if _, err := bw.WriteString("HELLO "); err != nil {
		return err
	}
	if _, err := bw.WriteString(strconv.Itoa(version)); err != nil {
		return err
	}
	if _, err := bw.WriteString(" OK\n"); err != nil {
		return err
	}
	return bw.Flush()
}

// WriteMsg writes one delivery frame: "MSG <topic> <partition> <msgID>
// <payloadLen>\n<payload>\n" and flushes. The partition is explicit because
// MsgIDs are partition-local (ADR 0021) and an all-partitions consumer must
// know which partition to ACK.
func WriteMsg(bw *bufio.Writer, topic string, partition int, msgID uint64, payload []byte) error {
	if _, err := bw.WriteString("MSG "); err != nil {
		return err
	}
	if _, err := bw.WriteString(topic); err != nil {
		return err
	}
	if err := bw.WriteByte(' '); err != nil {
		return err
	}
	if _, err := bw.WriteString(strconv.Itoa(partition)); err != nil {
		return err
	}
	if err := bw.WriteByte(' '); err != nil {
		return err
	}
	if _, err := bw.WriteString(strconv.FormatUint(msgID, 10)); err != nil {
		return err
	}
	if err := bw.WriteByte(' '); err != nil {
		return err
	}
	if _, err := bw.WriteString(strconv.Itoa(len(payload))); err != nil {
		return err
	}
	if err := bw.WriteByte('\n'); err != nil {
		return err
	}
	if _, err := bw.Write(payload); err != nil {
		return err
	}
	if err := bw.WriteByte('\n'); err != nil {
		return err
	}
	return bw.Flush()
}

// WriteErr writes "ERR <code> <reason>\n" and flushes. Codes are
// caller-defined (INVALID, PUB_FAILED, SUB_FAILED, ...).
func WriteErr(bw *bufio.Writer, code, reason string) error {
	if _, err := bw.WriteString("ERR "); err != nil {
		return err
	}
	if _, err := bw.WriteString(code); err != nil {
		return err
	}
	if err := bw.WriteByte(' '); err != nil {
		return err
	}
	if _, err := bw.WriteString(reason); err != nil {
		return err
	}
	if err := bw.WriteByte('\n'); err != nil {
		return err
	}
	return bw.Flush()
}

// WriteDup writes "DUP <originalMsgID>\n" and flushes — the response
// when a PUB hits the dedupe index. The caller learns the original
// MsgID and can treat the publish as idempotently successful.
func WriteDup(bw *bufio.Writer, originalMsgID uint64) error {
	if _, err := bw.WriteString("DUP "); err != nil {
		return err
	}
	if _, err := bw.WriteString(strconv.FormatUint(originalMsgID, 10)); err != nil {
		return err
	}
	if err := bw.WriteByte('\n'); err != nil {
		return err
	}
	return bw.Flush()
}
