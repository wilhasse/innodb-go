# Implementation Status and Gaps

This document summarizes the current Go port status against the original C
InnoDB codebase and highlights major missing features. Line counts are a
rough indicator and are taken from `wc -l` over each Go package directory.

## Implementation Status

| Subsystem | C Lines (approx) | Go Lines (approx) | Status | Notes |
| --- | --- | --- | --- | --- |
| btr (B-tree) | ~7,000 | 3,794 | Partial | Core tree ops, splits, cursors, trace harness; still in-memory and simplified. |
| dict (dictionary) | ~4,000 | 1,380 | Partial | SYS_* rows persisted to `ib_dict.sys` and reloaded on startup (not B-tree stored). |
| page (page format) | ~3,000 | 660 | Partial | Page headers + record list basics only. |
| fil (files) | ~3,500 | 460 | Minimal | Tablespace registry with attached files; page read/write helpers auto-extend size. |
| buf (buffer pool) | ~8,000 | 1,120 | Minimal | Simplified pool + LRU; fetch/flush uses fil page IO. |
| log (redo log) | ~3,000 | 900 | Partial | File-backed header + append-only records; checkpoint LSN persisted; recovery scan populates recv hash (LSN-only apply). |
| lock (locking) | ~5,700 | 352 | Minimal | No row locks, no deadlock detection. |
| trx (transactions) | ~4,500 | 1,273 | Minimal | IDs + scaffolding only, no real isolation or rollback. |
| row (row ops) | ~7,000 | 1,900 | Partial | Basic row ops + BTR integration; row-store log replay on attach. |
| read (read views) | ~500 | 194 | Minimal | No MVCC snapshots. |
| rem (record mgr) | ~2,000 | 378 | Partial | Basic record format helpers only. |
| rec | ~2,000 | 620 | Partial | Record encoding/decoding, headers, and comparison helpers. |
| undo | ~2,000 | 0 | Missing | Not implemented. |
| purge | ~1,500 | 0 | Missing | Not implemented. |

## Missing Core Features

1) MVCC (Multi-Version Concurrency Control)
- No undo logs for rollback
- No read views for snapshot isolation
- No version chains on records
- No purge of old versions

2) Transaction System
- No real transaction isolation (READ UNCOMMITTED only)
- No rollback capability
- No savepoints
- No distributed transaction support (XA)

3) Locking
- No row-level locks
- No gap locks / next-key locks
- No deadlock detection
- No lock wait queues

4) Durability
- Redo log persistence is minimal (header + append-only records)
- Crash recovery only updates page LSN, not page contents
- Minimal checkpoint LSN persistence
- No doublewrite buffer

5) Buffer Pool
- No flush lists
- No read-ahead / prefetch
- No adaptive hash integration with buffer pool pages
- No buffer pool instances

6) Background Goroutines
- No master goroutine
- No purge goroutine
- No page cleaner goroutine
- No log writer goroutine

## Why Go Tests May Be Faster

Without full durability, MVCC, and locking, the Go port runs as a simplified
single-threaded B-tree with minimal bookkeeping. Each operation skips:

1) Lock acquisition
2) Redo logging
3) Undo logging
4) Buffer pool LRU/flush work
5) MVCC read view checks
6) Transaction bookkeeping

The current API path now writes .ibd files, but it is still far from the
full I/O, recovery, and concurrency behavior of the C engine.

## Roadmap Priority

1) Durability: redo log, checkpoint, recovery
2) MVCC: undo logs, read views, purge
3) Locking: row locks, deadlock detection
4) Buffer Pool: LRU, flush, read-ahead
5) Background: master, purge, page cleaner, log writer

## Reference

C source location: `/home/cslog/oss-embedded-innodb`

Key C files for missing features:
- MVCC: `trx/trx0undo.c`, `read/read0read.c`
- Locking: `lock/lock0lock.c`
- Redo: `log/log0log.c`, `log/log0recv.c`
- Buffer: `buf/buf0lru.c`, `buf/buf0flu.c`

## Current Capability

The Go port should be able to:
- Create/drop databases and tables
- Persist schema metadata across restart (`ib_dict.sys`)
- Insert/update/delete rows via BTR
- Scan tables with cursors
- Create .ibd tablespace files
- Handle page splits
- Persist redo log headers and scan log records on startup

It cannot:
- Survive a crash (no recovery)
- Handle concurrent transactions
- Rollback failed transactions
- Provide isolation between readers/writers
