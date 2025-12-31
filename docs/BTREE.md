# B-tree Notes

## IBGO-136: Shared index storage + page registry
- C refs: `btr/btr0btr.c`, `btr/btr0cur.c`, `dict/dict0dict.c`
- Go mapping:
  - `row.Store` now owns a shared `btr.Tree` with encoded keys and row ID values.
  - `api.Cursor` iterates/searches via the shared tree to keep cursors consistent.
  - `page.Registry` provides an in-memory page lookup by space and page number.

## IBGO-137: Page create + record list
- C refs: `btr/btr0btr.c`, `page/page0page.c`, `rem/rec0rec.c`
- Go mapping:
  - `btr.PageCreate`/`btr.PageEmpty` initialize pages with infimum/supremum records.
  - `page.Record` tracks record type/heap number, and page cursors skip system records.
  - `rem` exports record type + heap number constants.

## IBGO-138: Node pointers + parent navigation
- C refs: `btr/btr0btr.c` (node pointer + father helpers)
- Go mapping:
  - `btr.NodePtrSetChildPageNo`/`btr.NodePtrGetChild` store child page numbers in node pointer records.
  - `page.Page` tracks `ParentPageNo` for parent lookups via `page.Registry`.
  - `btr.PageGetFatherBlock`/`btr.PageGetFatherNodePtr`/`btr.PageGetFather` walk parent linkage.

## IBGO-139: Page alloc/free + size
- C refs: `btr/btr0btr.c` (page alloc/free, btr_get_size), `fsp/fsp0fsp.c`, `buf/buf0buf.c`
- Go mapping:
  - `fsp.AllocPage`/`fsp.FreePage` manage a per-space free list and grow `fil.Space` size.
  - `btr.PageAlloc`/`btr.PageFree` wire allocation to `page.Registry` and optional `buf` pool.
  - `btr.GetSize` counts allocated pages per space via the registry.

## IBGO-140: Tree create/root management
- C refs: `btr/btr0btr.c` (btr_create, root get/free)
- Go mapping:
  - `btr.Create` allocates a root page and initializes `dict.Index` root metadata.
  - `btr.RootBlockGet`/`btr.RootGet` fetch the root page via the registry.
  - `btr.FreeRoot`/`btr.FreeButNotRoot` release pages and keep index metadata in sync.
