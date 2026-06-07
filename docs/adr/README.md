# Architecture Decision Records

| #    | Title                                                | Status   |
| ---- | ---------------------------------------------------- | -------- |
| 0001 | [Framed record format for the WAL](./0001-framed-record-format.md) | Accepted |
| 0002 | [Per-message fsync with atomic committed offset](./0002-per-message-fsync.md) | Accepted |
| 0003 | [Crash recovery by full segment scan](./0003-recovery-by-scan.md) | Accepted |

Each ADR is a short snapshot of *why* a decision was made, captured at the
time the decision landed in code. They are not living docs — when a decision
is overturned, write a new ADR that supersedes the old one rather than
editing history.
