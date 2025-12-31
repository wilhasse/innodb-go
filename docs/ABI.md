# C ABI Compatibility

This project defines a small C compatibility layer in `ut` that mirrors core
InnoDB base types and constants.

## Types

- `ut.Ulint` matches the native word size (same size and alignment as
  `uintptr`).
- `ut.IBool` matches C `ib_bool_t` (alias of `Ulint`).
- `ut.Dulint` packs two `Ulint` fields, mirroring `struct dulint_struct`.

When cgo is enabled, `ut.Ulint` and `ut.IBool` are derived from `uintptr_t`
via a cgo typedef to keep ABI alignment with C. When cgo is disabled, the
types fall back to Go `uintptr`.

## Constants

- `UNIV_PAGE_SIZE` and `UnivPageSize` are 16 KiB (shift 14), matching
  `UNIV_PAGE_SIZE` in `univ.i`.

## Endianness

`ut.NativeEndian` is detected at init time using `unsafe` and can be used by
packing/unpacking helpers to mirror C byte order assumptions.

## ABI Guarantees

The `ut` package validates at startup that `Ulint` matches the size and
alignment of `uintptr` and that `Dulint` is packed as two `Ulint` words. If
these guarantees are not met, the process panics early to avoid silent
corruption.
