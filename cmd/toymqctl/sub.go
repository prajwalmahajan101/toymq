package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/prajwalmahajan101/toymq/internal/config"
	"github.com/prajwalmahajan101/toymq/pkg/client"
)

func runSub(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sub", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", config.DefaultAddr, "broker address")
	noAutoAck := fs.Bool("no-auto-ack", false, "do not ACK messages automatically")
	maxMsgs := fs.Int("max-msgs", 0, "exit after N messages (0 = unbounded)")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: toymqctl sub [flags] <topic> <consumer-id>")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 2 {
		fs.Usage()
		return exitUsage
	}
	topic, consumerID := fs.Arg(0), fs.Arg(1)

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	c, err := client.Dial(dialCtx, *addr)
	cancel()
	if err != nil {
		fmt.Fprintf(stderr, "toymqctl sub: dial: %v\n", err)
		return exitErr
	}
	defer c.Close()

	ch, err := c.Sub(ctx, topic, consumerID)
	if err != nil {
		fmt.Fprintf(stderr, "toymqctl sub: %v\n", err)
		return exitErr
	}

	seen := 0
	for {
		select {
		case <-ctx.Done():
			return exitOK
		case d, ok := <-ch:
			if !ok {
				if errors.Is(c.Err(), client.ErrTransport) {
					fmt.Fprintf(stderr, "toymqctl sub: %v\n", c.Err())
					return exitErr
				}
				return exitOK
			}
			fmt.Fprintf(stdout, "MSG topic=%s id=%d payload=%q\n",
				d.Topic, d.MsgID, d.Payload)
			if !*noAutoAck {
				if err := d.Ack(ctx); err != nil {
					if errors.Is(err, context.Canceled) {
						return exitOK
					}
					fmt.Fprintf(stderr, "toymqctl sub: ack: %v\n", err)
					return exitErr
				}
			}
			seen++
			if *maxMsgs > 0 && seen >= *maxMsgs {
				return exitOK
			}
		}
	}
}
