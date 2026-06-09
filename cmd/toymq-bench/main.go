package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/prajwalmahajan101/toymq/internal/config"
)

const (
	exitOK    = 0
	exitErr   = 1
	exitUsage = 2
)

// benchConfig is the resolved set of CLI knobs. Validation happens
// in parseFlags so run() can be tested without touching os.Exit.
type benchConfig struct {
	Addr      string
	Topic     string
	Producers int
	Msgs      int
	Size      int
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	cfg, code := parseFlags(args, stderr)
	if code != exitOK {
		return code
	}

	// Chunks 2+3 replace this stub with the real bench loop.
	fmt.Fprintf(stdout, "toymq-bench  addr=%s  topic=%s  producers=%d  msgs=%d  size=%d\n",
		cfg.Addr, cfg.Topic, cfg.Producers, cfg.Msgs, cfg.Size)
	_ = ctx
	return exitOK
}

func parseFlags(args []string, stderr io.Writer) (benchConfig, int) {
	fs := flag.NewFlagSet("toymq-bench", flag.ContinueOnError)
	fs.SetOutput(stderr)

	cfg := benchConfig{}
	fs.StringVar(&cfg.Addr, "addr", config.DefaultAddr, "broker address")
	fs.StringVar(&cfg.Topic, "topic", "bench", "topic to publish to")
	fs.IntVar(&cfg.Producers, "producers", 4, "concurrent producer goroutines")
	fs.IntVar(&cfg.Msgs, "msgs", 10000, "total messages across all producers")
	fs.IntVar(&cfg.Size, "size", 256, "payload byte size")

	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: toymq-bench [flags]")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return benchConfig{}, exitUsage
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "toymq-bench: unexpected positional arg %q\n", fs.Arg(0))
		fs.Usage()
		return benchConfig{}, exitUsage
	}
	if cfg.Producers <= 0 {
		fmt.Fprintf(stderr, "toymq-bench: --producers must be > 0 (got %d)\n", cfg.Producers)
		return benchConfig{}, exitUsage
	}
	if cfg.Msgs <= 0 {
		fmt.Fprintf(stderr, "toymq-bench: --msgs must be > 0 (got %d)\n", cfg.Msgs)
		return benchConfig{}, exitUsage
	}
	if cfg.Size < 0 {
		fmt.Fprintf(stderr, "toymq-bench: --size must be >= 0 (got %d)\n", cfg.Size)
		return benchConfig{}, exitUsage
	}
	return cfg, exitOK
}
