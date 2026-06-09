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

// WriteMsg writes one delivery frame: "MSG <topic> <msgID>
// <payloadLen>\n<payload>\n" and flushes.
func WriteMsg(bw *bufio.Writer, topic string, msgID uint64, payload []byte) error {
	if _, err := bw.WriteString("MSG "); err != nil {
		return err
	}
	if _, err := bw.WriteString(topic); err != nil {
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
