package server

import (
	"crypto/subtle"
	"fmt"
	"os"
	"strings"
)

// serverMaxVersion is the highest HELLO wire version this build speaks.
// The handshake negotiates min(clientVersion, serverMaxVersion); today
// that is always 1. Bumped to 2 for v3 M7 push frames. See ADR 0020.
const serverMaxVersion = 1

// authConfig is the per-Server handshake policy applied to every
// Session. Its zero value — requireHello=false, no tokens — is exactly
// the pre-M3 behaviour (no handshake, no auth), so a Server constructed
// without options is unchanged.
type authConfig struct {
	requireHello bool
	tokens       []string // empty => auth disabled
}

// authEnabled reports whether a valid AUTH token is required in HELLO.
func (a authConfig) authEnabled() bool { return len(a.tokens) > 0 }

// checkToken reports whether tok matches any configured token. It
// compares against every token with crypto/subtle.ConstantTimeCompare
// and no early exit, so neither a match nor its position leaks through
// timing. Returns false when auth is disabled.
func (a authConfig) checkToken(tok string) bool {
	var matched int
	for _, t := range a.tokens {
		matched |= subtle.ConstantTimeCompare([]byte(tok), []byte(t))
	}
	return matched == 1
}

// LoadTokens reads a bearer-token file: one token per line, blank lines
// and lines beginning with '#' ignored. Returns an error if the file
// cannot be read or contains no usable tokens.
func LoadTokens(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read auth token file %s: %w", path, err)
	}
	var tokens []string
	for _, line := range strings.Split(string(data), "\n") {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "#") {
			continue
		}
		tokens = append(tokens, t)
	}
	if len(tokens) == 0 {
		return nil, fmt.Errorf("auth token file %s: no tokens found", path)
	}
	return tokens, nil
}
