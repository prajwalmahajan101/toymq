package main

import (
	"flag"

	"github.com/prajwalmahajan101/toymq/pkg/client"
)

// connFlags registers the shared connection flags (auth + TLS) on a
// flagset so every subcommand (pub/sub/ack) accepts them uniformly, and
// resolves them into client.Dial options.
type connFlags struct {
	authToken   *string
	tls         *bool
	tlsCA       *string
	tlsInsecure *bool
}

func registerConnFlags(fs *flag.FlagSet) *connFlags {
	return &connFlags{
		authToken:   fs.String("auth-token", "", "bearer token sent in the HELLO handshake"),
		tls:         fs.Bool("tls", false, "dial over TLS"),
		tlsCA:       fs.String("tls-ca", "", "PEM CA file trusted for -tls (empty = system roots)"),
		tlsInsecure: fs.Bool("tls-insecure", false, "skip TLS verification (dev/self-signed only)"),
	}
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
