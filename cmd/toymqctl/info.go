package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sort"

	"github.com/prajwalmahajan101/toymq/internal/config"
)

func runInfo(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("info", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", config.DefaultAddr, "broker address")
	conn := registerConnFlags(fs)
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: toymqctl info [flags]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 0 {
		fs.Usage()
		return exitUsage
	}

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	c, err := conn.dial(dialCtx, *addr)
	if err != nil {
		fmt.Fprintf(stderr, "toymqctl info: dial: %v\n", err)
		return exitErr
	}
	defer c.Close()

	ri, err := c.Info(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "toymqctl info: %v\n", err)
		return exitErr
	}
	// Print every key:value line, sorted for stable output.
	keys := make([]string, 0, len(ri.Raw))
	for k := range ri.Raw {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(stdout, "%s:%s\n", k, ri.Raw[k])
	}
	return exitOK
}
