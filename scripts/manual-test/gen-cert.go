//go:build ignore

// gen-cert.go is a throwaway self-signed cert generator for the manual-test
// lab. It writes a PEM cert and key valid for 127.0.0.1/localhost so the lab
// broker can terminate TLS without an openssl dependency. Run standalone:
//
//	go run gen-cert.go <cert-out.pem> <key-out.pem>
//
// It is NOT part of the toymq build (the //go:build ignore tag excludes it);
// it only ever runs via `go run` from tmux-lab.sh.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: go run gen-cert.go <cert-out.pem> <key-out.pem>")
		os.Exit(2)
	}
	certPath, keyPath := os.Args[1], os.Args[2]

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must(err)

	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"toymq manual-test lab"}, CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	must(err)

	certOut, err := os.Create(certPath)
	must(err)
	must(pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}))
	must(certOut.Close())

	keyDER, err := x509.MarshalECPrivateKey(key)
	must(err)
	keyOut, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	must(err)
	must(pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	must(keyOut.Close())

	fmt.Printf("wrote %s and %s (valid for 127.0.0.1, localhost)\n", certPath, keyPath)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "gen-cert:", err)
		os.Exit(1)
	}
}
