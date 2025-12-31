# Logging and Errors

The Go API mirrors the C `ib_logger` and `ib_err_t` layer.

## Logger

`api.Logger` is a hook compatible with `fprintf`-style usage:

```go
api.LoggerSet(api.DefaultLogger, os.Stderr)
api.Log(nil, "InnoDB: %s\n", "starting")
```

The default stream is stored in `api.LogStream`.

## Errors

`api.ErrCode` mirrors `enum db_err` and `api.ErrString` mirrors
`ib_strerror`. Use `api.Err(code)` to convert `DB_SUCCESS` to `nil` and
all other codes to an error.
