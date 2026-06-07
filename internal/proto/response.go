package proto

import (
	"bufio"
	"fmt"
	"strconv"
)

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

func WriteMsg(bw *bufio.Writer, topic string, msgID uint64, payload []byte) error {
	_, err := fmt.Fprintf(bw, "MSG %s %d %d\n", topic, msgID, len(payload))
	if err != nil {
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
