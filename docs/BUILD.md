# Build and Test

Basic test flow:

```sh
go test ./...
```

Build tags:

```sh
go test -tags debug ./...
go test -tags release ./...
```

The active build mode is exposed via `ut.BuildMode`, `ut.BuildDebug`, and
`ut.BuildRelease`.
