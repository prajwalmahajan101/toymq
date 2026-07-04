package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

const (
	exitOK    = 0
	exitErr   = 1
	exitUsage = 2
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return exitUsage
	}
	verb, rest := args[0], args[1:]
	switch verb {
	case "create":
		return runCreate(ctx, rest, stdout, stderr)
	case "pub":
		return runPub(ctx, rest, stdout, stderr)
	case "sub":
		return runSub(ctx, rest, stdout, stderr)
	case "ack":
		return runAck(ctx, rest, stdout, stderr)
	case "-h", "--help", "help":
		usage(stdout)
		return exitOK
	default:
		fmt.Fprintf(stderr, "toymqctl: unknown command %q\n\n", verb)
		usage(stderr)
		return exitUsage
	}
}

func usage(w io.Writer) {
	fmt.Fprint(w, `usage: toymqctl <command> [flags] <args...>

Commands:
  create <topic>                  create a topic (use -partitions N)
  pub <topic> <payload>           publish a message
  sub <topic> <consumer-id>       subscribe and stream messages (topic#n or topic#* to scope)
  ack <topic> <consumer-id> <id>  acknowledge one message id

Run "toymqctl <command> -h" for command-specific flags.
`)
}
