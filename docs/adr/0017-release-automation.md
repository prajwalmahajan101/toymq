# 0017 — Release automation with GoReleaser

**Status:** Accepted
**Date:** 2026-06-09
**Scope:** `.goreleaser.yaml`, `.github/workflows/release.yml`,
`cmd/*/main.go` (version vars)

## Context

After v1.0.0 the project shipped three more semver tags
(`v1.1.0` Interactive TUI, `v1.2.0` Godoc + opt-in slog,
`v1.3.0` Prometheus + OpenTelemetry observability) — but only
`v1.0.0` had a corresponding GitHub Release with notes and
binaries. The other three tags were "orphan releases": visible
in `git tag` but invisible to users browsing the Releases page
or downloading binaries.

Hand-rolling each release worked once; it does not scale to
four binaries × two operating systems × two architectures = 16
archives per tag, with checksums, changelog grouping, and
upload retries.

## Decision

Adopt **GoReleaser** (`v2` line) driven by a GitHub Actions
workflow that fires on `v*.*.*` tag pushes.

`.goreleaser.yaml` defines four `builds:` entries (`toymq`,
`toymqctl`, `toymq-bench`, `toymq-tui`), each cross-compiled for
linux/darwin × amd64/arm64 with reproducible flags
(`-trimpath`, `-s -w`, ldflag-injected `main.version`,
`main.commit`, `main.date`). Archives are `tar.gz`, include
`README.md` + `LICENSE`, and a `SHA256SUMS` file is generated
per release.

Changelog grouping by Conventional Commit prefix (`feat`,
`fix`, `docs`, `chore`) is computed from `git log` between tags.

Releases are created as **drafts**. The maintainer reviews the
generated notes and publishes manually. This matches the
existing v1.0.0 workflow's curation step and prevents
mis-titled or empty releases from going live.

The three orphan tags (v1.1.0/v1.2.0/v1.3.0) are backfilled
manually with `goreleaser release --snapshot` in temporary
worktrees, since their commits predate the workflow.

## Consequences

**Positive**
- Future tags auto-publish to the Releases page with binaries
  for the four most common Unix targets.
- Reproducible builds (`-trimpath`, no CGO) verifiable from the
  SHA256SUMS file shipped alongside.
- Changelog is generated from the canonical source (git log of
  Conventional Commits) so it does not drift from history.

**Negative**
- One more YAML file and one more workflow to maintain.
- Draft-first means the maintainer must manually press
  "Publish" — easy to forget after a tag push.
- Backfilled releases (v1.1.0/v1.2.0/v1.3.0) are reproduced
  from current `.goreleaser.yaml` against historical source.
  Asset *content* matches the tag commit, but anyone who
  rebuilds from source at the tag may see minor differences
  (build-flags, ldflag values) unless they replicate the
  GoReleaser invocation.

## Deferred

- **Signing.** Cosign keyless signing via GitHub OIDC is the
  natural next step. Deferred because (a) no downstream user
  has asked for it yet, and (b) it adds a verification step
  the project's README would need to teach, complicating the
  install path. Revisit when the project has external
  consumers.
- **Docker image push to GHCR.** The CI workflow already builds
  the image on every push; adding a tag-triggered push to
  `ghcr.io/prajwalmahajan101/toymq` is mechanical. Deferred
  until a downstream wants pre-built images.
- **Homebrew tap.** Out of scope for v1.

## Usage

Cut a release:
```
git tag -a v1.4.0 -m "v1.4.0 — <theme>"
git push origin v1.4.0
```

The workflow runs GoReleaser, creates a draft release, attaches
16 archives + `SHA256SUMS`. Review the draft on the Releases
page, edit the notes if needed, and publish.

Local dry-run (no upload):
```
goreleaser release --snapshot --clean --skip=publish
```

Output lands in `dist/`. Verify before tagging.
