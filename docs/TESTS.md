# C Test Runner

This repository includes a Go runner that builds and executes the original C
tests from `oss-embedded-innodb/tests`.

## Build prerequisites

The C tests link against a built/installed libinnodb:

- headers at `<prefix>/include/embedded_innodb-1.0`
- library at `<prefix>/lib`

Build and install the C library first (from `oss-embedded-innodb`), then run
the Go wrapper.

## Run from Go

```sh
go run ./cmd/ctests
```

Common options:

- `-tests ib_test1,ib_test2` to run a subset
- `-all` to run the default suite
- `-build-only` to only build the C tests

Environment overrides:

- `INNODB_C_TESTS_DIR` (defaults to `../oss-embedded-innodb/tests`)
- `INNODB_C_TESTS_PREFIX` (used as `TOP` for Makefile.examples)
- `INNODB_C_TESTS_LIBDIR` (prepended to `LD_LIBRARY_PATH`)

## Notes

- Some tests require cleaning data/log files between runs.
- On Windows, use the CMake/VS build flow and set `PATH` instead of
  `LD_LIBRARY_PATH`.
- Recovery coverage includes restart scan of redo logs in `api/recovery_test.go`.
- MVCC/rollback coverage lives in `go test ./api` (read views, savepoints, purge).

## MVCC Test Coverage

The API test suite now exercises:

- consistent read visibility for insert/update/delete
- rollback (full + savepoint)
- purge when read views close

## Test Comparison (Post-IBGO-195)

This section tracks the Go vs C test timing after the SYS metadata
persistence stabilizing fixes landed. Fill in the "Current" column after
running the full sweep.

| Metric | Previous | Current |
| --- | --- | --- |
| C Tests | TBD | TBD |
| Go Tests | TBD | TBD |
| Speedup | N/A | TBD |

Reference (Zig baseline):

| Metric | C Tests | Zig Tests |
| --- | --- | --- |
| Time | 590s | 51s |
| Speedup |  | 11.56x faster |

Stabilizing fixes included:
- Dedup SYS_* rows on load/insert to avoid duplicates across runs
- Auto-extend tablespace size on page writes
- Shutdown cleanup even if startup was skipped
- Schema reload from persisted SYS rows on startup

Expected metadata files (when `data_home_dir` is set):

```
ib_dict.sys       (dictionary persistence)
ib_logfile0       (redo log header + records)
<db>/<table>.ibd  (table storage log)
```
