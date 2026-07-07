package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/wal"
)

// Config is the validated flag bundle passed from the broker binary
// into broker.New / server.New. Construct via Parse so validation
// runs; never zero-value it. See ADR 0009.
type Config struct {
	Addr            string
	DataDir         string
	LogLevel        string
	LogFormat       string
	ShutdownTimeout time.Duration
	DedupeCap       int

	// RecvWindow is the per-(partition,consumer) receive window: the
	// broker delivers at most this many un-acked messages to a consumer
	// before pausing delivery until an ACK frees a slot (ADR 0022). A
	// SUB #* across N partitions is therefore bounded by N*RecvWindow.
	// Min 1.
	RecvWindow int

	// DefaultPartitions is the partition count applied to a topic
	// auto-created by a first PUB/SUB (ADR 0021). Existing on-disk topics
	// keep their recovered count; CREATE overrides per topic. Min 1.
	DefaultPartitions int

	// FsyncMode selects the WAL durability strategy (ADR 0019):
	// per-message (default) | batched | none. FsyncInterval is the
	// group-commit window and applies only to batched.
	FsyncMode     string
	FsyncInterval time.Duration

	// WAL retention (ADR 0023). SegmentBytes caps one WAL segment; 0
	// keeps a single unbounded segment and disables retention. RetainBytes
	// and RetainDuration bound per-partition disk use and both require
	// SegmentBytes > 0 (there must be sealed segments to reclaim). All
	// default to 0 (off), preserving pre-M6 behaviour.
	SegmentBytes   int64
	RetainBytes    int64
	RetainDuration time.Duration

	// Handshake / auth / TLS (ADR 0020). RequireHello makes the HELLO
	// frame mandatory (default); AuthTokenFile enables bearer-token
	// auth; TLSAddr runs a TLS listener alongside the plain Addr using
	// TLSCert/TLSKey. All default to the pre-M3 posture (hello required,
	// no auth, no TLS) except that a fresh binary now requires HELLO.
	RequireHello  bool
	AuthTokenFile string
	TLSAddr       string
	TLSCert       string
	TLSKey        string

	// Observability (ADR 0015). Empty MetricsAddr disables the
	// HTTP /metrics + /healthz endpoint. Empty OTLPEndpoint
	// installs the noop tracer. Both default to off so existing
	// deployments are unaffected.
	MetricsAddr      string
	OTLPEndpoint     string
	TraceSampleRatio float64
	ServiceVersion   string
}

// Default flag values exported so cmd binaries (toymqctl, toymq-bench,
// toymq-tui) and tests share one source of truth.
const (
	DefaultAddr              = ":6789"
	DefaultDataDir           = "./data"
	DefaultLogLevel          = "info"
	DefaultLogFormat         = "text"
	DefaultShutdownTimeout   = 5 * time.Second
	DefaultDedupeCap         = 4096
	DefaultRecvWindow        = 256
	DefaultDefaultPartitions = 1
	DefaultFsyncMode         = "per-message"
	DefaultFsyncInterval     = wal.DefaultSyncInterval
	DefaultRequireHello      = true
	DefaultMetricsAddr       = ""
	DefaultOTLPEndpoint      = ""
	DefaultTraceSampleRatio  = 0.05
	DefaultServiceVersion    = "dev"
)

var (
	validLogLevels  = map[string]struct{}{"debug": {}, "info": {}, "warn": {}, "error": {}}
	validLogFormats = map[string]struct{}{"text": {}, "json": {}}
)

// Parse reads args (typically os.Args[1:]) and produces a validated
// Config. Output destined for the user (e.g. -h help text) goes to
// stderr. Errors are returned, not printed.
func Parse(args []string, stderr io.Writer) (*Config, error) {
	fs := flag.NewFlagSet("toymq", flag.ContinueOnError)
	fs.SetOutput(stderr)

	cfg := &Config{}
	fs.StringVar(&cfg.Addr, "addr", DefaultAddr, "TCP listen address")
	fs.StringVar(&cfg.DataDir, "data-dir", DefaultDataDir, "broker storage root")
	fs.StringVar(&cfg.LogLevel, "log-level", DefaultLogLevel, "debug|info|warn|error")
	fs.StringVar(&cfg.LogFormat, "log-format", DefaultLogFormat, "text|json")
	fs.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", DefaultShutdownTimeout, "graceful drain budget")
	fs.IntVar(&cfg.DedupeCap, "dedupe-cap", DefaultDedupeCap, "per-topic dedupe LRU size")
	fs.IntVar(&cfg.RecvWindow, "recv-window", DefaultRecvWindow, "per-consumer receive window: max un-acked messages before delivery pauses (>=1)")
	fs.IntVar(&cfg.DefaultPartitions, "default-partitions", DefaultDefaultPartitions, "partition count for auto-created topics (>=1)")
	fs.StringVar(&cfg.FsyncMode, "fsync", DefaultFsyncMode, "WAL durability: per-message|batched|none")
	fs.DurationVar(&cfg.FsyncInterval, "fsync-interval", DefaultFsyncInterval, "group-commit window for -fsync=batched")
	fs.Int64Var(&cfg.SegmentBytes, "segment-bytes", 0, "WAL segment size cap in bytes; 0 keeps a single unbounded segment (disables retention)")
	fs.Int64Var(&cfg.RetainBytes, "retain-bytes", 0, "max retained WAL bytes per partition; 0 = unbounded (requires -segment-bytes)")
	fs.DurationVar(&cfg.RetainDuration, "retain-duration", 0, "drop WAL segments whose newest record is older than this, per partition; 0 = unbounded (requires -segment-bytes)")
	fs.BoolVar(&cfg.RequireHello, "require-hello", DefaultRequireHello, "require the HELLO handshake as the first frame (false = plaintext migration window)")
	fs.StringVar(&cfg.AuthTokenFile, "auth-token-file", "", "file of bearer tokens (one per line) enabling AUTH; empty disables auth")
	fs.StringVar(&cfg.TLSAddr, "tls-addr", "", "TLS listen address, run alongside -addr; empty disables TLS")
	fs.StringVar(&cfg.TLSCert, "tls-cert", "", "PEM certificate file for -tls-addr")
	fs.StringVar(&cfg.TLSKey, "tls-key", "", "PEM private key file for -tls-addr")
	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", DefaultMetricsAddr, "Prometheus /metrics listen address (empty disables)")
	fs.StringVar(&cfg.OTLPEndpoint, "otlp-endpoint", DefaultOTLPEndpoint, "OTLP gRPC tracing endpoint (empty disables tracing)")
	fs.Float64Var(&cfg.TraceSampleRatio, "trace-sample-ratio", DefaultTraceSampleRatio, "fraction of root spans to sample [0..1]")
	fs.StringVar(&cfg.ServiceVersion, "service-version", DefaultServiceVersion, "value reported as otel.service.version")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if c.Addr == "" {
		return errors.New("addr must not be empty")
	}
	if c.DataDir == "" {
		return errors.New("data-dir must not be empty")
	}
	if _, ok := validLogLevels[c.LogLevel]; !ok {
		return fmt.Errorf("log-level %q: must be debug|info|warn|error", c.LogLevel)
	}
	if _, ok := validLogFormats[c.LogFormat]; !ok {
		return fmt.Errorf("log-format %q: must be text|json", c.LogFormat)
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("shutdown-timeout %v: must be > 0", c.ShutdownTimeout)
	}
	if c.DedupeCap <= 0 {
		return fmt.Errorf("dedupe-cap %d: must be > 0", c.DedupeCap)
	}
	if c.RecvWindow < 1 {
		return fmt.Errorf("recv-window %d: must be >= 1", c.RecvWindow)
	}
	if c.DefaultPartitions < 1 {
		return fmt.Errorf("default-partitions %d: must be >= 1", c.DefaultPartitions)
	}
	if _, err := wal.ParseSyncMode(c.FsyncMode); err != nil {
		return fmt.Errorf("fsync %q: %w", c.FsyncMode, err)
	}
	if c.FsyncMode == "batched" && c.FsyncInterval <= 0 {
		return fmt.Errorf("fsync-interval %v: must be > 0 for -fsync=batched", c.FsyncInterval)
	}
	if c.SegmentBytes < 0 {
		return fmt.Errorf("segment-bytes %d: must be >= 0", c.SegmentBytes)
	}
	if c.RetainBytes < 0 {
		return fmt.Errorf("retain-bytes %d: must be >= 0", c.RetainBytes)
	}
	if c.RetainDuration < 0 {
		return fmt.Errorf("retain-duration %v: must be >= 0", c.RetainDuration)
	}
	if (c.RetainBytes > 0 || c.RetainDuration > 0) && c.SegmentBytes <= 0 {
		return errors.New("retain-bytes/retain-duration require -segment-bytes > 0")
	}
	if c.TraceSampleRatio < 0 || c.TraceSampleRatio > 1 {
		return fmt.Errorf("trace-sample-ratio %v: must be in [0,1]", c.TraceSampleRatio)
	}
	if (c.TLSCert == "") != (c.TLSKey == "") {
		return errors.New("tls-cert and tls-key must be set together")
	}
	if c.TLSAddr != "" && c.TLSCert == "" {
		return errors.New("tls-addr requires tls-cert and tls-key")
	}
	return nil
}
