# Go Porting Conventions

This document captures the conventions for porting embedded InnoDB C code to Go.

## Naming

- Package names mirror C subsystem directories (`btr`, `buf`, `trx`, etc.).
- Function names prefer Go style (CamelCase) within their package. Avoid
  duplicating C prefixes that are already expressed by the package name.
- Keep public API names in `api` aligned with existing C API concepts where
  it improves discoverability (`ErrCode`, `Logger`, etc.).

## Package Boundaries

- Follow `docs/PACKAGES.md` for ownership and responsibilities.
- Avoid cross-package circular dependencies; use `ut` for shared helpers.
- Add new packages only when the C headers provide a clear separation.

## C Interop Rules

- Use `ut` for C-compat base types (`Ulint`, `IBool`, `UNIV_PAGE_SIZE`).
- Prefer `[]byte` for on-disk page buffers; keep packing helpers isolated.
- Restrict `unsafe` and `cgo` usage to compatibility and low-level helpers.

## Error Handling

- Use `api.ErrCode` for InnoDB error codes and `api.ErrString` for messages.
- Public API functions should return `ErrCode` (or `error` via `api.Err`).
- Internal helpers may return `error` but should wrap/convert to `ErrCode`
  at the API boundary.

## Logging

- Use `api.Logger` and `api.Log` to emit engine messages.
- Avoid direct stdout/stderr writes from ported code.

## Testing

- Default: `go test ./...`
- C tests: `go run ./cmd/ctests` (see `docs/TESTS.md`)
- Clean data/log artifacts between C test runs.
