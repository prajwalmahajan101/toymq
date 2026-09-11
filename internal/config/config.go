package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"
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

	// DLQAfterNacks moves a message to <topic>.dlq once it has failed this
	// many delivery attempts (nacks or visibility timeouts). 0 disables the
	// dead-letter queue (ADR 0024).
	DLQAfterNacks int

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

	// Replication (v3 M1, ADR 0028). Replicate routes every mutating command
	// through raft Propose→Apply; false (default) is the standalone path,
	// byte-identical to v2. NodeID is this node's raft identity. RaftDir is
	// the raft log/state store; empty defaults to <data-dir>/raft. M1 is
	// single-node only (Peers == [self]); multi-node peer wiring is M2.
	//
	// Peers is the raw --peers flag: "id@baseURL,id@baseURL,…", the full
	// cluster membership including self (v3 M2, ADR 0030). Empty keeps the M1
	// single-node path. RaftAddr is this node's raft-transport listen address
	// (host:port); required when Peers is set.
	Replicate bool
	NodeID    string
	RaftDir   string
	Peers     string
	RaftAddr  string
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
	DefaultNodeID            = "n1"
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
	fs.IntVar(&cfg.DLQAfterNacks, "dlq-after-nacks", 0, "move a message to <topic>.dlq after this many failed deliveries; 0 disables the dead-letter queue")
	fs.BoolVar(&cfg.RequireHello, "require-hello", DefaultRequireHello, "require the HELLO handshake as the first frame (false = plaintext migration window)")
	fs.StringVar(&cfg.AuthTokenFile, "auth-token-file", "", "file of bearer tokens (one per line) enabling AUTH; empty disables auth")
	fs.StringVar(&cfg.TLSAddr, "tls-addr", "", "TLS listen address, run alongside -addr; empty disables TLS")
	fs.StringVar(&cfg.TLSCert, "tls-cert", "", "PEM certificate file for -tls-addr")
	fs.StringVar(&cfg.TLSKey, "tls-key", "", "PEM private key file for -tls-addr")
	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", DefaultMetricsAddr, "Prometheus /metrics listen address (empty disables)")
	fs.StringVar(&cfg.OTLPEndpoint, "otlp-endpoint", DefaultOTLPEndpoint, "OTLP gRPC tracing endpoint (empty disables tracing)")
	fs.Float64Var(&cfg.TraceSampleRatio, "trace-sample-ratio", DefaultTraceSampleRatio, "fraction of root spans to sample [0..1]")
	fs.StringVar(&cfg.ServiceVersion, "service-version", DefaultServiceVersion, "value reported as otel.service.version")
	fs.BoolVar(&cfg.Replicate, "replicate", false, "route mutating commands through embedded raft (v3 M1, single-node); false = standalone")
	fs.StringVar(&cfg.NodeID, "node-id", DefaultNodeID, "raft node identity for -replicate")
	fs.StringVar(&cfg.RaftDir, "raft-dir", "", "raft log/state directory for -replicate; empty defaults to <data-dir>/raft")
	fs.StringVar(&cfg.Peers, "peers", "", "cluster membership incl self for -replicate: id@baseURL,…; empty = single-node (v3 M2)")
	fs.StringVar(&cfg.RaftAddr, "raft-addr", "", "this node's raft transport listen address host:port; required with -peers")

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
	if c.DLQAfterNacks < 0 {
		return fmt.Errorf("dlq-after-nacks %d: must be >= 0", c.DLQAfterNacks)
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
	if c.Replicate && c.NodeID == "" {
		return errors.New("node-id must not be empty with -replicate")
	}
	if c.Replicate && c.Peers != "" {
		peers, err := ParsePeers(c.Peers)
		if err != nil {
			return fmt.Errorf("peers: %w", err)
		}
		if _, ok := peers[c.NodeID]; !ok {
			return fmt.Errorf("peers must contain this node's own id %q (self must appear in the membership)", c.NodeID)
		}
		if len(peers)%2 == 0 {
			return fmt.Errorf("peers must be odd for a clean majority, got %d (even N can split quorum)", len(peers))
		}
		if c.RaftAddr == "" {
			return errors.New("raft-addr must be set with -peers")
		}
	}
	return nil
}

// ParsePeers parses the --peers flag ("id@baseURL,id@baseURL,…") into a
// NodeID→base-URL map. The map is string-keyed so this package stays free of a
// toyraft import; cmd converts to raft.NodeID at assembly. Entries are
// comma-separated; each is "id@url" with a non-empty id and a parseable
// absolute URL. A duplicate id is an error (a later entry silently overwriting
// an earlier one is a config bug, not a merge).
func ParsePeers(raw string) (map[string]string, error) {
	out := make(map[string]string)
	for entry := range strings.SplitSeq(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			return nil, fmt.Errorf("empty entry in %q", raw)
		}
		id, rawURL, ok := strings.Cut(entry, "@")
		if !ok || id == "" || rawURL == "" {
			return nil, fmt.Errorf("entry %q: want id@baseURL", entry)
		}
		u, err := url.Parse(rawURL)
		if err != nil {
			return nil, fmt.Errorf("entry %q: %w", entry, err)
		}
		if u.Scheme == "" || u.Host == "" {
			return nil, fmt.Errorf("entry %q: baseURL must be absolute (scheme://host)", entry)
		}
		if _, dup := out[id]; dup {
			return nil, fmt.Errorf("duplicate node id %q", id)
		}
		out[id] = rawURL
	}
	return out, nil
}

// ClusterPeers returns the parsed --peers map, or nil when Peers is empty
// (single-node path). Assumes validate has already run, so it ignores the parse
// error a validated config cannot produce.
func (c *Config) ClusterPeers() map[string]string {
	if c.Peers == "" {
		return nil
	}
	peers, _ := ParsePeers(c.Peers)
	return peers
}

// PeerURLsExcludingSelf returns the peer→URL map with this node removed, as the
// http transport's PeerURLs wants (it never Sends to itself). nil for the
// single-node path.
func (c *Config) PeerURLsExcludingSelf() map[string]string {
	peers := c.ClusterPeers()
	if peers == nil {
		return nil
	}
	delete(peers, c.NodeID)
	return peers
}

// ResolvedRaftDir returns the raft store directory: RaftDir when set, else
// <data-dir>/raft. Only meaningful under -replicate.
func (c *Config) ResolvedRaftDir() string {
	if c.RaftDir != "" {
		return c.RaftDir
	}
	return filepath.Join(c.DataDir, "raft")
}
