package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
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
	Addr string
	// Peers, when set, switches the bench to cluster mode: each producer dials
	// the comma-separated members ("id@host:port" or "host:port") through a
	// redirect-following client.ClusterClient (v3 M3, ADR 0032) and load is
	// measured against the leader. Empty = standalone against Addr. The report
	// labels the run so standalone vs replicated numbers are self-documenting.
	Peers     string
	Topic     string
	Producers int
	Msgs      int
	Size      int
	// Partitions, when > 1, is created on the topic before the run so
	// keyless publishes round-robin across partitions (ADR 0021). The
	// report labels the run so partitioned vs single-log runs are
	// self-documenting.
	Partitions int
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

// dialConn opens one producer connection. With -peers set it returns a
// redirect-following ClusterClient (cluster mode); otherwise a single-connection
// Client against -addr (standalone). Both satisfy publisher.
func dialConn(ctx context.Context, cfg benchConfig, opts []client.Option) (publisher, error) {
	if cfg.Peers != "" {
		return client.DialCluster(ctx, splitPeers(cfg.Peers), opts...)
	}
	return client.Dial(ctx, cfg.Addr, opts...)
}

// splitPeers turns "id@host:port, host:port,…" into a trimmed member slice,
// dropping empty entries. DialCluster accepts both the id-tagged and bare forms.
func splitPeers(raw string) []string {
	var out []string
	for m := range strings.SplitSeq(raw, ",") {
		if m = strings.TrimSpace(m); m != "" {
			out = append(out, m)
		}
	}
	return out
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

	conns := make([]publisher, cfg.Producers)
	for i := 0; i < cfg.Producers; i++ {
		dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
		c, err := dialConn(dialCtx, cfg, dialOpts)
		cancel()
		if err != nil {
			fmt.Fprintf(stderr, "toymq-bench: dial #%d: %v\n", i, err)
			for j := 0; j < i; j++ {
				_ = conns[j].Close()
			}
			return exitErr
		}
		conns[i] = c
	}
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()

	// Create the topic with the requested partition count before producing
	// so keyless publishes round-robin across partitions.
	if cfg.Partitions > 1 {
		createCtx, cancel := context.WithTimeout(ctx, dialTimeout)
		err := conns[0].Create(createCtx, cfg.Topic, cfg.Partitions)
		cancel()
		if err != nil {
			fmt.Fprintf(stderr, "toymq-bench: create topic: %v\n", err)
			return exitErr
		}
	}

	per := distribute(cfg.Msgs, cfg.Producers)
	payload := makePayload(cfg.Size)
	results := make([]result, cfg.Producers)

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < cfg.Producers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = runProducer(ctx, conns[i], cfg.Topic, per[i], payload)
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
	fs.StringVar(&cfg.Addr, "addr", config.DefaultAddr, "broker address (standalone mode)")
	fs.StringVar(&cfg.Peers, "peers", "", "comma-separated cluster members (id@host:port,...) — enables redirect-following cluster mode; overrides -addr")
	fs.StringVar(&cfg.Topic, "topic", "bench", "topic to publish to")
	fs.IntVar(&cfg.Producers, "producers", 4, "concurrent producer goroutines")
	fs.IntVar(&cfg.Msgs, "msgs", 10000, "total messages across all producers")
	fs.IntVar(&cfg.Size, "size", 256, "payload byte size")
	fs.IntVar(&cfg.Partitions, "partitions", 1, "create the topic with N partitions before the run (>=1)")
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
