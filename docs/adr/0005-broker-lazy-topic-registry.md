# 0005 — Lazy topic registry with double-checked locking

**Status:** Accepted
**Date:** 2026-06-08
**Scope:** `internal/broker/broker.go`, `internal/broker/topic.go`

## Context

A topic in ToyMQ owns a WAL segment, a dedupe index, and a consumer
registry. The broker holds many topics keyed by name. Two operational
questions shaped the design:

1. **Where do topics come from?** Most production brokers require
   explicit creation. ToyMQ aims to be a toy: publishing to an unknown
   topic should just create it.
2. **What about startup?** Existing topics on disk must be re-opened
   (so their WALs run recovery and their offsets are loaded) before
   the broker is "ready," but we want concurrent first-publishers to
   not race each other into double-opening the same WAL.

## Decision

Two-tier laziness:

1. **On `Broker.New`** — scan `data/topics/` and pre-open every
   existing topic. Each gets `wal.Open` (which runs recovery) and
   `Topic.loadOffsets` (which seeds consumer state). After this, every
   pre-existing topic is in the registry.
2. **On any unknown name** (Publish or Subscribe) — `getOrCreateTopic`
   creates the topic on demand using **double-checked locking**:

   ```go
   b.mu.RLock(); t, ok := b.topics[name]; b.mu.RUnlock()
   if ok { return t, nil }

   b.mu.Lock()
   defer b.mu.Unlock()
   if t, ok := b.topics[name]; ok { return t, nil } // re-check
   t = newTopic(...); b.topics[name] = t
   return t, nil
   ```

The same shape recurs at `Topic.getOrCreateConsumer` for consumer
lifetimes inside a topic.

## Consequences

**Positive**
- Lock-free reads on the happy path (`RLock`), which is what the
  Publish hot path takes.
- No leaked WAL handles when two goroutines simultaneously publish to
  a new topic — the re-check under the write lock catches the race.
- Fresh brokers and reopened brokers behave identically from the
  caller's perspective. No "topic doesn't exist" error path on
  first-write or first-read.
- Per-topic state and locks (`pubMu`, `consumersMu`, dedupe, WAL) are
  cleanly scoped under the Topic. No cross-topic interference.

**Negative**
- Producers can typo a topic name and silently create a new one
  forever. Acceptable for v1; a production broker would gate creation
  behind ACLs.
- The startup `os.ReadDir` is O(topics-on-disk). At thousands of
  topics this becomes a measurable open-handle cost. Documented as a
  deferred concern; segment rotation and lazy-only loading are the
  fixes when needed.

## Edge cases the code accounts for

- **Empty `data/topics/`** — `os.IsNotExist` returns the empty broker
  rather than an error.
- **Non-directory entries** — `entries` includes files; we skip
  anything where `!e.IsDir()` so stray files (`.DS_Store`, lock files
  from another tool) don't confuse the scan.
- **Initial typo in the implementation** that bypassed the
  `b.topics[name] = t` assignment — caught during code review. Would
  have leaked an FD per publish until `EMFILE`. Captured in the build
  journal as a representative "tests don't catch this; reads do."

## Usage

- All entry points (`Publish`, `Subscribe`, `Ack`, `Nack`) go through
  `getOrCreateTopic`. There is no separate `CreateTopic` API.
- Future code that lists topics for monitoring must take `b.mu.RLock()`
  for the iteration.
