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

## IBGO-141: Cursor search to nth level
- C refs: `btr/btr0cur.c` (btr_cur_search_to_nth_level, btr_cur_add_path_info, open-at-side/random)
- Go mapping:
  - `Cur.SearchToNthLevel` traverses the in-memory tree, positions the cursor, and captures path info.
  - `Cur.OpenAtIndexSide`/`Cur.OpenAtRandom` now record path metadata for future splits.

## IBGO-142: Cursor navigation across pages
- C refs: `btr/btr0btr.c` (btr_get_prev_user_rec, btr_get_next_user_rec), `btr/btr0pcur.c`
- Go mapping:
  - `btr.GetNextUserRec`/`btr.GetPrevUserRec` step across linked pages via `page.Page` prev/next pointers.

## IBGO-143: Optimistic leaf insert
- C refs: `btr/btr0cur.c` (btr_cur_insert_if_possible, btr_cur_optimistic_insert)
- Go mapping:
  - `Cur.InsertIfPossible` inserts into a leaf when there is capacity and rejects duplicates.
  - `Cur.OptimisticInsert` wraps the insert with lock/undo/report placeholders for the in-memory tree.

## IBGO-144: Page split + root raise
- C refs: `btr/btr0btr.c` (split selection, insert fits, split insert, root raise)
- Go mapping:
  - `PageGetSplitRecToLeft/Right` and `PageGetSureSplitRec` choose split points on `page.Page`.
  - `PageSplitAndInsert` wraps in-memory inserts and reports when a split is needed.
  - `RootRaiseAndInsert` detects height increases when root splits.

## IBGO-145: Non-leaf insert + node pointers
- C refs: `btr/btr0btr.c` (non-leaf insert, node ptr maintenance)
- Go mapping:
  - `InsertOnNonLeafLevel`, `AttachHalfPages`, `NodePtrDelete`, and `LiftPageUp` wrap internal node pointer updates.
  - Tree insertion now updates parent separator keys when leaf minima change.

## IBGO-146: Delete marking + delete
- C refs: `btr/btr0cur.c` (del-mark, optimistic/pessimistic delete)
- Go mapping:
  - `Cur.DelMarkSetClustRec`/`Cur.DelMarkSetSecRec` set delete marks; `Cur.DelUnmarkForIbuf` clears them.
  - `Cur.OptimisticDelete`/`Cur.PessimisticDelete` perform physical deletes.
  - Cursor navigation skips delete-marked keys for visibility.

## IBGO-147: Update paths
- C refs: `btr/btr0cur.c` (update-in-place, optimistic/pessimistic update)
- Go mapping:
  - `Cur.UpdateInPlace`, `Cur.OptimisticUpdate`, `Cur.PessimisticUpdate` implement size-preserving and size-changing updates.
  - `Cur.UpdateAllocZip` triggers best-effort compaction; `Cur.ParseUpdateInPlace` defers to `UpdateInPlace`.

## IBGO-148: Page reorg/compress
- C refs: `btr/btr0btr.c` (reorg/compress/discard)
- Go mapping:
  - `PageReorganizeLow`/`PageReorganize` purge delete-marked keys.
  - `Compress` and `DiscardPage` compact/free in-memory pages; level helpers are no-ops without a level list.

## IBGO-149: Persistent cursor
- C refs: `btr/btr0pcur.c`
- Go mapping:
  - `Pcur.MoveToNextPage`/`Pcur.MoveBackwardFromPage` move across leaf pages.
  - `Pcur.OpenOnUserRecFunc` aliases open on user rec helpers.

## IBGO-150: Estimate + size
- C refs: `btr/btr0cur.c` (estimate helpers), `btr/btr0btr.c` (size)
- Go mapping:
  - `EstimateNRowsInRange` and `EstimateNumberOfDifferentKeyVals` scan visible keys for deterministic estimates.

## IBGO-151: External fields / BLOB
- C refs: `btr/btr0cur.c` (extern field helpers)
- Go mapping:
  - `StoreBigRecExternFields` stores large values in `fil` external storage with a compact ref.
  - `RecGetExternallyStoredLen`, `CopyExternallyStoredFieldPrefix`, and `RecFreeExternallyStoredFields` manage refs.

## IBGO-152: Adaptive hash index
- C refs: `btr/btr0sea.c`
- Go mapping:
  - `SearchSysCreate`/`SearchSysClose` manage an in-tree hash index.
  - `SearchBuildPageHashIndex` and `SearchGuessOnHash` provide deterministic hash lookups.

## IBGO-153: Validation + integration tests
- C refs: `btr/btr0btr.c` (btr_check_node_ptr, btr_validate_index)
- Go mapping:
  - `ValidateIndex` and `CheckNodePtr` enforce ordering and leaf-link invariants.
  - Integration test in `tests/btr_validate_test.go` exercises random insert/update/delete.

## IBGO-155: BTR trace comparison harness
- C refs: `btr/btr0btr.c`, `btr/btr0cur.c`
- Go mapping:
  - `TraceOperations` runs a deterministic B-tree operation log with final key list.
  - `cmd/btrtrace` prints the trace for comparison with the C reference harness.
  - `btr/trace_test.go` locks the trace output hash.

## IBGO-156: C trace harness + Go/C diff script
- C refs: `tests/ib_cursor.c`, `tests/ib_search.c`
- Go mapping:
  - `tools/c/btr_trace.c` emits the same trace using the embedded InnoDB API.
  - `scripts/btr_trace_diff.sh` builds the C harness, runs both traces, and diffs output.
