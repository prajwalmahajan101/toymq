# 0003 — Crash recovery by full segment scan

**Status:** Accepted
**Date:** 2026-06-06
**Scope:** `internal/wal/recovery.go`

## Context

After a crash (or a clean shutdown), the broker must reconstruct:

- `nextMsgID` — the next ID to assign on the next `Append`.
- `committed` — the byte offset past the last fully-valid record.
- The file's true tail — any partially-written record from a writer crash
  must be truncated so subsequent appends do not produce a gap.

We need a strategy that is correct first, fast second. The toy is single-node
and v1 expects modest log sizes.

## Decision

On `Open`, scan the segment file from byte 0 forward, decoding records
sequentially via the same `Decode` used at runtime. Track three values
during the scan:

- `pos` — running byte cursor (sum of consumed bytes per Decode).
- `lastGood` — byte position immediately after the last successfully-decoded
  record.
- `lastID` — `MsgID` of the last successfully-decoded record.

Three branches:

1. **`Decode` returns `io.EOF`** — the file ended cleanly on a record boundary.
   Break out of the loop. No truncation needed.
2. **`Decode` returns `ErrShortRead`, `ErrBadCRC`, or `ErrTooLarge`** —
   the tail is torn. Call `Truncate(lastGood)` on the read-write file handle
   and break.
3. **`Decode` returns any other error** — real I/O failure. Bubble it up;
   `Open` will close the file and return.

After the loop:

- `nextMsgID = lastID + 1` if any record was seen (using `lastGood > 0` as
  the "we saw something" flag — important because `MsgID == 0` is valid).
- `committed = lastGood`.

The scan uses a *separate* read-only file handle from the read-write handle
used for appends. Truncate is called on the read-write handle so it picks up
the new size.

## Consequences

**Positive**
- One source of truth: `Decode` is the runtime parser and the recovery
  parser. A bug in either is caught at the other.
- Recovery state matches the on-disk reality byte-for-byte. No external
  index can go stale.
- All three "torn tail" cases collapse to one truncate. Simpler than
  trying to classify "writer crashed mid-flush" vs "single-bit rot" vs
  "garbage appended out of band" — the response is the same.

**Negative**
- O(file size) on every startup. Acceptable while we run with one segment
  per topic and bounded sizes; will become painful at GB-scale logs. The
  fix when we get there is segment rotation + an index, both deferred.
- The full payload of every record is read and CRC-checked on startup,
  not just the headers. We could skip payload bytes with `Seek` + a
  header-only CRC scheme but that doubles the encoding complexity for a
  v1 toy. Not worth it yet.

## Edge cases the code accounts for

- **First-ever record has `MsgID == 0`.** Initial code used
  `if lastID > 0`, which would have left `nextMsgID == 0` and produced
  duplicate IDs on the next append. Now uses `if lastGood > 0`, which only
  cares whether the scan consumed at least one valid record.
- **Empty file.** Decode returns `io.EOF` on the first call. `lastGood`
  stays 0, `nextMsgID` stays 0 (its zero value), `committed` stays 0.
  Next append starts at MsgID 0 at byte offset 0.
- **Trailing garbage.** Tested in `TestRecoveryTruncatesTornTail`: append
  five records cleanly, write 7 random bytes, reopen, verify the file
  size is back to the pre-garbage size and `nextMsgID == 5`.

## Usage

- The scan runs once per `Open`. No background recovery; no incremental
  checkpoints. The file *is* the checkpoint.
- Future code that touches the segment outside of `Append` (e.g.,
  compaction, rotation) must run with the writer mutex held and re-trigger
  scan-based recovery before allowing further appends.
