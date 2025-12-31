# Memory Allocation Strategy

This document outlines how the C `mem0mem` layer maps to Go memory management.

## Goals

- Preserve semantics while relying on Go's GC for long-lived objects.
- Use `sync.Pool` for short-lived, fixed-size buffers (pages, log blocks).
- Keep allocation interfaces explicit so call sites can be migrated in stages.

## Allocator Interface

The Go port uses a small allocator interface:

```go
type Allocator interface {
    Alloc(size int) []byte
    AllocZero(size int) []byte
    Free(buf []byte)
}
```

`mem.DefaultAllocator` is a GC-backed implementation where `Free` is a no-op.

## sync.Pool Usage

Use `mem.BufferPool` for fixed-size buffers:

- 16 KiB pages (`UNIV_PAGE_SIZE`)
- 512 B log blocks
- Common tuple/record scratch buffers

Pooling is intentionally limited to predictable, fixed-size buffers to avoid
unbounded memory retention.

## Mapping Plan (C -> Go)

- `mem_alloc` / `mem_zalloc` -> `Allocator.Alloc` / `Allocator.AllocZero`
- `mem_free` -> `Allocator.Free` (no-op under GC, pooled where relevant)
- `mem_heap_create` / `mem_heap_alloc` -> `mem.Heap` (planned) with chunk lists
  backed by `BufferPool` for common sizes
- `mem_heap_free` -> drop references so GC can reclaim

## Migration Plan

1) Land allocator interface and buffer pools (this stage).
2) Introduce `mem.Heap` abstraction and migrate `mem_heap_*` call sites.
3) Add size-classed pools for hot paths and page/log buffers.
4) Audit for large allocations and ensure they bypass pooling.
