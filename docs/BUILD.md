# Build and Test

Basic test flow:

```sh
go test ./...
```

Build tags:

```sh
go test -tags debug ./...
go test -tags release ./...
go test -tags nocompression ./...
go test -tags noshared ./...
go test -tags atomic_gcc ./...
go test -tags atomic_solaris ./...
go test -tags atomic_innodb ./...
```

Defaults:
- compression enabled (use `nocompression` to disable)
- shared enabled (use `noshared` to disable)
- atomic ops auto selection (override with `atomic_gcc`, `atomic_solaris`, or
  `atomic_innodb`)

Build settings are exposed via `ut.BuildMode`, `ut.BuildDebug`,
`ut.BuildRelease`, `ut.CompressionEnabled`, `ut.SharedEnabled`, and
`ut.AtomicOpsMode`.
