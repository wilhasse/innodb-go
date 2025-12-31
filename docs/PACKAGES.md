# Package Boundaries

This document summarizes the intended boundaries for each Go package. The
layout mirrors the top-level subsystem directories in oss-embedded-innodb.

## Foundations
- ut: common utilities, assertions, and small helpers
- mach: byte order and integer packing/unpacking
- os: OS abstraction for files, paths, and time
- sync: latches, mutexes, and rw-locks
- thr: goroutine and threading helpers
- mem: memory accounting and allocator wrappers
- dyn: dynamic arrays and list primitives

## Storage and logging
- fil: file space and tablespace management
- fsp: file space headers and extent allocation
- fut: file space free list helpers
- log: redo log records and log writer/reader
- mtr: mini-transaction logging and buffering

## Buffer and page layout
- buf: buffer pool, LRU, flush lists, and page caching
- page: page headers and layout helpers
- rem: record format helpers
- ibuf: insert buffer

## Index and dictionary
- btr: B+ tree index and cursor logic
- data: data types and field handling
- dict: data dictionary and metadata
- ddl: DDL operations
- eval: expression evaluation utilities

## Row, transaction, and locking
- row: row operations and record handling
- trx: transaction system and undo
- lock: lock system
- que: query graph and execution
- pars: SQL parser and lexer

## Server and API
- srv: server lifecycle and background threads
- api: public embedded InnoDB API
- ha: storage engine hooks for tests/examples
- usr: user-facing helpers

## Misc
- read: low-level read helpers
