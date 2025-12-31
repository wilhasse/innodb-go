# Go Porting Roadmap and Package Map

This document maps the embedded InnoDB C subsystems to Go packages and
defines a staged porting order with test milestones.

## Package Map (C -> Go)

Base utilities
- ut -> ut: common helpers, assertions, error handling helpers
- mach -> mach: byte order, integer packing/unpacking
- os -> os: file I/O, path handling, time
- sync -> sync: latches, mutexes, rw-locks
- thr -> thr: goroutine/threading helpers
- mem -> mem: memory accounting/allocator wrappers
- dyn -> dyn: dynamic array and list primitives

Storage and logging
- fil -> fil: file space and tablespace management
- fsp -> fsp: file space headers, extent allocation
- fut -> fut: file space free list helpers
- log -> log: redo log records, log writer/reader
- mtr -> mtr: mini-transaction logging and redo buffering

Buffer + page layout
- buf -> buf: buffer pool, LRU, flush lists
- page -> page: page headers, page format helpers
- rem -> rem: record (tuple) format helpers
- ibuf -> ibuf: insert buffer

Index + dictionary
- btr -> btr: B+ tree index and cursors
- dict -> dict: data dictionary, table/index metadata
- data -> data: data types and field handling
- ddl -> ddl: DDL operations
- eval -> eval: expression evaluation utilities

Row/transaction/lock
- row -> row: row operations and record handling
- trx -> trx: transaction system and undo
- lock -> lock: lock system
- que -> que: query graph and execution
- pars -> pars: SQL parser/lexer

Server + API
- srv -> srv: server start/stop and background threads
- api -> api: public embedded InnoDB API
- ha -> ha: storage engine hooks (as needed for tests/examples)
- usr -> usr: user-facing helpers

Misc
- read -> read: low-level read helpers

## Dependency Sketch (high-level)

The core dependency direction should stay bottom-up:
- ut/mach/os/sync/thr are foundational.
- mem/dyn depend only on foundational packages.
- fil/fsp/fut/log/mtr depend on foundational + mem/dyn.
- buf/page/rem depend on fil/fsp/log/mtr.
- btr/dict/data/ddl/ibuf depend on buf/page/rem/log/mtr.
- trx/lock/row/que/pars depend on btr/dict/data/log/mtr.
- srv/api sit at the top and wire subsystems together.

## Incremental Porting Order

Phase 1: Foundations
1) ut, mach, os, sync, thr
2) mem, dyn

Phase 2: Storage plumbing
3) fil, fsp, fut
4) log, mtr

Phase 3: Buffer and page formats
5) buf, page, rem
6) ibuf (insert buffer)

Phase 4: Index and dictionary
7) btr (tree, cursor, search)
8) data, dict, ddl

Phase 5: Transactions and row layer
9) trx, lock
10) row, que, pars

Phase 6: Server + API
11) srv
12) api, ha, usr (public API and harness)

## Test Milestones

M1: Unit tests for foundational packages (ut/mach/os/sync).
M2: File space and redo log tests (fil/fsp/log/mtr).
M3: Buffer/page structure tests (buf/page/rem).
M4: B+ tree cursor/search tests (btr).
M5: Transaction/lock correctness tests (trx/lock/row).
M6: Port and run C tests from oss-embedded-innodb/tests via Go harness.
