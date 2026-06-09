package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/config"
	"github.com/prajwalmahajan101/toymq/pkg/client"
)

const dialTimeout = 5 * time.Second

func runPub(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("pub", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", config.DefaultAddr, "broker address")
	key := fs.String("key", "", "dedupe key (optional)")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: toymqctl pub [flags] <topic> <payload>")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 2 {
		fs.Usage()
		return exitUsage
	}
	topic, payload := fs.Arg(0), fs.Arg(1)

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	c, err := client.Dial(dialCtx, *addr)
	if err != nil {
		fmt.Fprintf(stderr, "toymqctl pub: dial: %v\n", err)
		return exitErr
	}
	defer c.Close()

	id, dup, err := c.Pub(ctx, topic, *key, []byte(payload))
	if err != nil {
		fmt.Fprintf(stderr, "toymqctl pub: %v\n", err)
		return exitErr
	}
	if dup {
		fmt.Fprintf(stdout, "DUP %d\n", id)
	} else {
		fmt.Fprintf(stdout, "OK %d\n", id)
	}
	return exitOK
}
