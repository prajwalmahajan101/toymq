package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"

	"github.com/prajwalmahajan101/toymq/internal/config"
	"github.com/prajwalmahajan101/toymq/pkg/client"
)

func runCreate(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", config.DefaultAddr, "broker address")
	partitions := fs.Int("partitions", 1, "number of partitions (>=1)")
	conn := registerConnFlags(fs)
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: toymqctl create [flags] <topic>")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return exitUsage
	}
	if *partitions < 1 {
		fmt.Fprintf(stderr, "toymqctl create: partitions must be >= 1, got %d\n", *partitions)
		return exitUsage
	}
	topic := fs.Arg(0)

	opts, err := conn.dialOptions()
	if err != nil {
		fmt.Fprintf(stderr, "toymqctl create: %v\n", err)
		return exitUsage
	}

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	c, err := client.Dial(dialCtx, *addr, opts...)
	cancel()
	if err != nil {
		fmt.Fprintf(stderr, "toymqctl create: dial: %v\n", err)
		return exitErr
	}
	defer c.Close()

	if err := c.Create(ctx, topic, *partitions); err != nil {
		fmt.Fprintf(stderr, "toymqctl create: %v\n", err)
		return exitErr
	}
	fmt.Fprintf(stdout, "OK %s partitions=%s\n", topic, strconv.Itoa(*partitions))
	return exitOK
}
