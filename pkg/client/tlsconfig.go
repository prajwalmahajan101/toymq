package client

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSConfig builds a *tls.Config for dialing a TLS broker, suitable for
// passing to WithTLS. caFile, when non-empty, is a PEM file whose certs
// are trusted as roots (use this for a self-signed broker cert); empty
// uses the system roots. insecure disables certificate verification —
// dev/self-signed only, never in production. MinVersion is TLS 1.2.
func TLSConfig(caFile string, insecure bool) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if insecure {
		cfg.InsecureSkipVerify = true //nolint:gosec // explicit dev opt-in via --tls-insecure
		return cfg, nil
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read tls CA %s: %w", caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls CA %s: no certificates parsed", caFile)
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}
