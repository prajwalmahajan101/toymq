package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/config"
	"github.com/prajwalmahajan101/toymq/pkg/client"
)

const dialTimeout = 5 * time.Second

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
	// Fsync labels the run with the broker's WAL durability mode
	// (per-message|batched|none). The bench is a client and cannot set
	// the broker's mode; this is a record so per-message vs batched runs
	// are self-documenting and tabulatable (the README batched column).
	Fsync string

	AuthToken   string
	TLS         bool
	TLSCA       string
	TLSInsecure bool
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

// benchDialOptions turns the auth/TLS flags into client.Dial options.
func benchDialOptions(cfg benchConfig) ([]client.Option, error) {
	var opts []client.Option
	if cfg.AuthToken != "" {
		opts = append(opts, client.WithAuth(cfg.AuthToken))
	}
	if cfg.TLS || cfg.TLSCA != "" || cfg.TLSInsecure {
		tlsCfg, err := client.TLSConfig(cfg.TLSCA, cfg.TLSInsecure)
		if err != nil {
			return nil, err
		}
		opts = append(opts, client.WithTLS(tlsCfg))
	}
	return opts, nil
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	cfg, code := parseFlags(args, stderr)
	if code != exitOK {
		return code
	}

	dialOpts, err := benchDialOptions(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "toymq-bench: %v\n", err)
		return exitUsage
	}

	clients := make([]*client.Client, cfg.Producers)
	for i := 0; i < cfg.Producers; i++ {
		dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
		c, err := client.Dial(dialCtx, cfg.Addr, dialOpts...)
		cancel()
		if err != nil {
			fmt.Fprintf(stderr, "toymq-bench: dial #%d: %v\n", i, err)
			for j := 0; j < i; j++ {
				_ = clients[j].Close()
			}
			return exitErr
		}
		clients[i] = c
	}
	defer func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}()

	per := distribute(cfg.Msgs, cfg.Producers)
	payload := makePayload(cfg.Size)
	results := make([]result, cfg.Producers)

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < cfg.Producers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = runProducer(ctx, clients[i], cfg.Topic, per[i], payload)
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	stats := aggregate(results, cfg.Size, elapsed)
	writeReport(stdout, stats, cfg)
	if stats.Errors > 0 {
		return exitErr
	}
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
	fs.StringVar(&cfg.Fsync, "fsync", "per-message", "label the run with the broker's fsync mode: per-message|batched|none")
	fs.StringVar(&cfg.AuthToken, "auth-token", "", "bearer token sent in the HELLO handshake")
	fs.BoolVar(&cfg.TLS, "tls", false, "dial over TLS")
	fs.StringVar(&cfg.TLSCA, "tls-ca", "", "PEM CA file trusted for -tls (empty = system roots)")
	fs.BoolVar(&cfg.TLSInsecure, "tls-insecure", false, "skip TLS verification (dev/self-signed only)")

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
