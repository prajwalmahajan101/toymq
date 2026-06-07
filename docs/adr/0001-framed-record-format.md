# 0001 — Framed record format for the WAL

**Status:** Accepted
**Date:** 2026-06-06
**Scope:** `internal/wal/record.go`

## Context

The WAL stores every published message on disk so that durability and crash
recovery are possible. We need a single on-disk format that:

- can be appended without seeking,
- detects torn writes (writer crashed mid-flush) and bit-rot,
- distinguishes "clean end of log" from "torn tail" during recovery,
- is bounded in size so a hostile or buggy producer cannot exhaust memory,
- is cheap to encode and decode in the hot publish path.

## Decision

Each record is a length-prefixed frame with a trailing CRC-32 checksum:

```
[u32 length] [u64 msg_id] [u64 ts_ns] [u16 key_len] [key] [u32 payload_len] [payload] [u32 crc32]
```

- All multi-byte integers are **little-endian**, chosen for consistency and
  zero-cost on the platforms we run on (x86, ARM). Explicit byte order rather
  than `binary.NativeEndian` so logs are portable across machines.
- `length` is the count of bytes that follow it, **including** the trailing CRC.
- `crc32` is CRC-32 IEEE over the inner bytes (`msg_id` through `payload`).
- `MaxRecordSize = 4 MiB`. Validated *before* allocating the read buffer in
  `Decode` so an oversized length header is rejected without touching memory.

The encoder builds the inner bytes into a scratch `bytes.Buffer`, computes the
CRC once over `inner.Bytes()`, then writes `[length][inner][crc]` into the
caller's buffer.

The decoder distinguishes three error classes via sentinel errors:

| Sentinel       | Meaning                                                 |
| -------------- | ------------------------------------------------------- |
| `io.EOF`       | Zero bytes read at the start of a frame — clean end.    |
| `ErrShortRead` | Bytes read partway through a frame, then EOF — torn.    |
| `ErrBadCRC`    | Frame body did not match its stored CRC.                |
| `ErrTooLarge`  | Length header claims a record larger than MaxRecordSize.|

The split between `io.EOF` and `ErrShortRead` is load-bearing: recovery uses
the former to stop cleanly and the latter to truncate the tail.

## Consequences

**Positive**
- One write per record, no in-place updates. Trivially append-friendly.
- Recovery can walk forward without any external index.
- CRC detects every torn write we care about (writer crash, half-flushed
  block, single-bit rot).
- Bounded memory: malicious or corrupted length headers are caught before any
  allocation.

**Negative**
- No backward seek. Reading record N means scanning from byte 0. Acceptable
  for v1; a sparse index would be a stretch goal.
- CRC-32 is fine for accident detection but not for adversaries. We are not
  trying to defend against tampering, only torn writes — acceptable.
- The `u16` key length caps dedupe keys at 65535 bytes. Encoder refuses
  larger keys explicitly so the cap is enforced at the boundary, not silently
  via integer truncation.

## Usage

- Encoders MUST pass a `*bytes.Buffer` so the caller can reuse capacity.
- Decoders MUST be given a `*bufio.Reader` to avoid per-field syscalls.
- The Payload returned by `Decode` is a fresh allocation; callers may retain
  it past the next read.

## Tests that lock this contract

`internal/wal/record_test.go`:

- `TestRoundTripSimple`, `TestRoundTripSizes` — encode/decode invert each
  other across payload sizes from 0 bytes to near MaxRecordSize.
- `TestDecodeBadCRC` — bit-flip inside the protected region → `ErrBadCRC`.
- `TestDecodeShortRead` — last 7 bytes chopped → `ErrShortRead`.
- `TestDecodeCleanEOF` — empty input → `io.EOF`.
- `TestDecodeTooLarge` — hand-crafted oversized length → `ErrTooLarge`
  without allocation.
