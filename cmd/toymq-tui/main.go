package main

import (
	"context"
	"errors"
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
	authToken := fs.String("auth-token", "", "bearer token sent in the HELLO handshake")
	useTLS := fs.Bool("tls", false, "dial over TLS")
	tlsCA := fs.String("tls-ca", "", "PEM CA file trusted for -tls (empty = system roots)")
	tlsInsecure := fs.Bool("tls-insecure", false, "skip TLS verification (dev/self-signed only)")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: toymq-tui [--addr host:port] [--tls] [--auth-token t]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	var opts []client.Option
	if *authToken != "" {
		opts = append(opts, client.WithAuth(*authToken))
	}
	if *useTLS || *tlsCA != "" || *tlsInsecure {
		tlsCfg, err := client.TLSConfig(*tlsCA, *tlsInsecure)
		if err != nil {
			return fmt.Errorf("tls: %w", err)
		}
		opts = append(opts, client.WithTLS(tlsCfg))
	}

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	c, err := client.Dial(dialCtx, *addr, opts...)
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
