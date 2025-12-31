# OS Abstraction Notes

The Go port exposes minimal OS shims in `os`, `thr`, and `sync` to mirror the
C `os0*` and threading utilities.

## Files

`os.DefaultFS` uses the Go standard library and assumes POSIX-like semantics
for rename, mkdir, and read/write APIs. Windows support will rely on Go's
platform behavior and may require per-call adjustments when porting the C
code that expects POSIX guarantees.

## Goroutines

`thr.Go`, `thr.Sleep`, and `thr.Yield` wrap goroutines, timers, and scheduler
yielding. These are intended as thin shims for portability and testing.

## Synchronization

The `sync` package re-exports the Go standard library primitives. Any future
instrumentation (lock tracking, stats) should be added here to avoid touching
call sites.
