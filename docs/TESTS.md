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
