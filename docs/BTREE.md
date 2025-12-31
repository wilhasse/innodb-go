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
