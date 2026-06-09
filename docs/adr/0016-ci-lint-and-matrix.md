# 0016 — CI lint + Go version matrix + local hook

**Status:** Accepted
**Date:** 2026-06-09
**Scope:** `.github/workflows/ci.yml`, `.golangci.yml`, `Makefile`,
`.githooks/pre-commit`

## Context

`main` went RED twice from the same class of bug: `gofmt`
column-alignment drift introduced by a merge that no contributor
checked locally because there was no `Makefile` and no pre-commit
hook. The existing CI runs `gofmt -l`, but the failure is observed
*after* merge — not before push.

Beyond formatting, the project had no broader static-analysis pass.
`go vet` catches a narrow set of issues; tools like `errcheck`,
`staticcheck`, `unused`, `gosec`, and `ineffassign` catch real
bugs gofmt and vet miss (unhandled errors on `io.Closer`, dead code,
shadowed variables, predictable randomness in test seeds).

Finally, CI tested on a single Go version (`go.mod` pin: 1.26.3).
A 1.26-only language feature could land without anyone noticing
until a downstream user on 1.25 reported a build failure.

## Decision

Layered defenses:

1. **Local Makefile** (`make fmt`, `fmt-check`, `vet`, `lint`,
   `test`, `ci`, `hooks`) — contributors run the same checks CI
   runs.
2. **Opt-in pre-commit hook** at `.githooks/pre-commit`, installed
   via `make hooks`. Fast: `gofmt -l` on staged `.go` files plus
   `go vet ./...`. Anything slower (lint, full test suite) belongs
   in CI, not the hook.
3. **`golangci-lint`** with a conservative ruleset (`errcheck`,
   `gocritic`, `gofmt`, `gosec`, `govet`, `ineffassign`, `misspell`,
   `revive`, `staticcheck`, `unused`). Test files and the chaos
   suite get relaxed rules.
4. **Go version matrix** in CI: `1.25.x` and `1.26.x`. Catches
   accidental use of 1.26-only syntax.

## Consequences

**Positive**
- Same failure modes caught locally and in CI; no more "RED after
  merge" surprises for trivially fixable issues.
- Lint catches a broader bug class than `go vet` alone.
- Matrix protects downstream users on the previous Go release.

**Negative**
- CI `test` job wall-clock doubles (mitigated: matrix entries run
  in parallel; total pipeline time unchanged).
- New contributors who don't run `make hooks` get no local
  protection — CI is still the source of truth.
- `golangci-lint` ruleset will drift over time; revisions need a
  new ADR or amendment.

## Usage

Contributor onboarding (one-time):
```
make hooks
```

Before pushing:
```
make ci      # fmt-check + vet + lint + test
```

If `golangci-lint` is not installed locally, `make lint` prints
an install hint and exits non-zero. CI installs it via
`golangci/golangci-lint-action@v6`.

Adding or removing a linter: update `.golangci.yml`, run
`make lint` locally, fix or `//nolint:<rule> // <reason>` any new
findings, then update this ADR's ruleset list.
