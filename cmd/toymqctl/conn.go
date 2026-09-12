package main

import (
	"context"
	"flag"
	"strings"

	"github.com/prajwalmahajan101/toymq/pkg/client"
)

// brokerConn is the request surface shared by the single-connection
// client.Client and the redirect-following client.ClusterClient, so every
// subcommand works against either without caring which (v3 M3, ADR 0032).
type brokerConn interface {
	PubDelay(ctx context.Context, topic, dedupeKey, routingKey string, payload []byte, delayMs uint64) (uint64, bool, error)
	PubWait(ctx context.Context, topic, dedupeKey, routingKey string, payload []byte, waitReplicas int, waitTimeoutMs uint64) (uint64, bool, error)
	Info(ctx context.Context) (client.ReplicationInfo, error)
	Create(ctx context.Context, topic string, partitions int) error
	Ack(ctx context.Context, consumerID string, partition int, msgID uint64) error
	Nack(ctx context.Context, consumerID string, partition int, msgID uint64) error
	Sub(ctx context.Context, topic, consumerID string) (<-chan client.Delivery, error)
	SubStale(ctx context.Context, topic, consumerID string) (<-chan client.Delivery, error)
	Err() error
	Close() error
}

// connFlags registers the shared connection flags (auth + TLS + cluster) on a
// flagset so every subcommand (pub/sub/ack) accepts them uniformly, and
// resolves them into a broker connection.
type connFlags struct {
	authToken   *string
	tls         *bool
	tlsCA       *string
	tlsInsecure *bool
	cluster     *string
}

func registerConnFlags(fs *flag.FlagSet) *connFlags {
	return &connFlags{
		authToken:   fs.String("auth-token", "", "bearer token sent in the HELLO handshake"),
		tls:         fs.Bool("tls", false, "dial over TLS"),
		tlsCA:       fs.String("tls-ca", "", "PEM CA file trusted for -tls (empty = system roots)"),
		tlsInsecure: fs.Bool("tls-insecure", false, "skip TLS verification (dev/self-signed only)"),
		cluster:     fs.String("cluster", "", "comma-separated cluster members (id@host:port,...) for redirect-following; overrides -addr"),
	}
}

// dial builds a broker connection: a redirect-following ClusterClient when
// -cluster is set, else a direct single-connection Client to addr (standalone,
// unchanged).
func (c *connFlags) dial(ctx context.Context, addr string) (brokerConn, error) {
	opts, err := c.dialOptions()
	if err != nil {
		return nil, err
	}
	if *c.cluster != "" {
		members := strings.Split(*c.cluster, ",")
		return client.DialCluster(ctx, members, opts...)
	}
	return client.Dial(ctx, addr, opts...)
}

// dialOptions turns the parsed flags into client options. Returns an
// error if a TLS config cannot be built.
func (c *connFlags) dialOptions() ([]client.Option, error) {
	var opts []client.Option
	if *c.authToken != "" {
		opts = append(opts, client.WithAuth(*c.authToken))
	}
	if *c.tls || *c.tlsCA != "" || *c.tlsInsecure {
		cfg, err := client.TLSConfig(*c.tlsCA, *c.tlsInsecure)
		if err != nil {
			return nil, err
		}
		opts = append(opts, client.WithTLS(cfg))
	}
	return opts, nil
}
