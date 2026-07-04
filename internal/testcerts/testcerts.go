// Package testcerts generates ephemeral self-signed TLS material for
// tests. It imports no testing package so it can be used from any
// _test.go across the module (server, client, integration). Never use
// this for anything but tests — the keys live only in memory.
package testcerts

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"
)

// GenerateSelfSigned returns PEM-encoded cert and key for a self-signed
// certificate valid for the given hosts (IPs or DNS names). It defaults
// to 127.0.0.1 + localhost when no host is given.
func GenerateSelfSigned(hosts ...string) (certPEM, keyPEM []byte, err error) {
	if len(hosts) == 0 {
		hosts = []string{"127.0.0.1", "localhost"}
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("testcerts: gen key: %w", err)
	}

	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "toymq-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}

	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("testcerts: create cert: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("testcerts: marshal key: %w", err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, nil
}

// ServerConfig builds a *tls.Config presenting the given cert/key, min
// TLS 1.2.
func ServerConfig(certPEM, keyPEM []byte) (*tls.Config, error) {
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("testcerts: load keypair: %w", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}

// ClientConfig builds a *tls.Config that trusts certPEM as a root, so a
// client verifies the test server without InsecureSkipVerify.
func ClientConfig(certPEM []byte) (*tls.Config, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		return nil, fmt.Errorf("testcerts: append cert to pool")
	}
	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
}
