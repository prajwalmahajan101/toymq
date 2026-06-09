package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/prajwalmahajan101/toymq/internal/config"
	"github.com/prajwalmahajan101/toymq/pkg/client"
)

const dialTimeout = 5 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("toymq-tui", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", config.DefaultAddr, "broker address")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: toymq-tui [--addr host:port]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	c, err := client.Dial(dialCtx, *addr)
	cancel()
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer c.Close()

	m := newModel(ctx, c, *addr)
	p := tea.NewProgram(m,
		tea.WithOutput(stdout),
		tea.WithContext(ctx),
		tea.WithAltScreen(),
	)
	if _, err := p.Run(); err != nil {
		return fmt.Errorf("tui: %w", err)
	}
	return nil
}
