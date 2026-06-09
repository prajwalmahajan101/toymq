package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/config"
	"github.com/prajwalmahajan101/toymq/pkg/client"
)

const ackWaitTimeout = 5 * time.Second

func runAck(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("ack", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", config.DefaultAddr, "broker address")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: toymqctl ack [flags] <topic> <consumer-id> <msg-id>")
		fs.PrintDefaults()
		fmt.Fprintln(stderr, "")
		fmt.Fprintln(stderr, "Note: messages with lower ids that arrive before")
		fmt.Fprintln(stderr, "the target are left un-acked and will be redelivered.")
	}
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 3 {
		fs.Usage()
		return exitUsage
	}
	topic, consumerID := fs.Arg(0), fs.Arg(1)
	target, err := strconv.ParseUint(fs.Arg(2), 10, 64)
	if err != nil {
		fmt.Fprintf(stderr, "toymqctl ack: bad msg-id %q: %v\n", fs.Arg(2), err)
		return exitUsage
	}

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	c, err := client.Dial(dialCtx, *addr)
	cancel()
	if err != nil {
		fmt.Fprintf(stderr, "toymqctl ack: dial: %v\n", err)
		return exitErr
	}
	defer c.Close()

	// SUB to register the consumer and open the delivery stream.
	// The broker only accepts an ACK for a msg already marked
	// inflight (i.e. one it has actually delivered), so we must
	// wait for the target MSG to arrive before acking.
	ch, err := c.Sub(ctx, topic, consumerID)
	if err != nil {
		fmt.Fprintf(stderr, "toymqctl ack: sub: %v\n", err)
		return exitErr
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, ackWaitTimeout)
	defer waitCancel()
	for {
		select {
		case <-waitCtx.Done():
			fmt.Fprintf(stderr, "toymqctl ack: timed out waiting for msg %d\n", target)
			return exitErr
		case d, ok := <-ch:
			if !ok {
				if errors.Is(c.Err(), client.ErrTransport) {
					fmt.Fprintf(stderr, "toymqctl ack: %v\n", c.Err())
				} else {
					fmt.Fprintln(stderr, "toymqctl ack: client closed before target msg arrived")
				}
				return exitErr
			}
			if d.MsgID != target {
				// Not our target. Leave it un-acked so visibility
				// timeout redelivers it to whoever owns this consumer
				// next.
				continue
			}
			if err := d.Ack(ctx); err != nil {
				fmt.Fprintf(stderr, "toymqctl ack: %v\n", err)
				return exitErr
			}
			fmt.Fprintf(stdout, "OK %d\n", target)
			return exitOK
		}
	}
}
