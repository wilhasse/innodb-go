package tests

import (
	"fmt"
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

func TestSecondaryIndexScanOrder(t *testing.T) {
	resetAPI(t)
	if err := api.Init(); err != api.DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	defer func() {
		_ = api.Shutdown(api.ShutdownNormal)
	}()

	if err := api.Startup("barracuda"); err != api.DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}
	if err := api.DatabaseCreate("test"); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	tableName := "test/sec_idx"
	if err := createSecondaryIndexTable(tableName); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}

	initial := [][2]int32{{1, 30}, {2, 10}, {3, 20}}
	if err := insertSecondaryIndexRows(tableName, initial); err != api.DB_SUCCESS {
		t.Fatalf("insert rows: %v", err)
	}
	indexName := fmt.Sprintf("%s_%s", tableName, "c2")
	if err := createSecondaryIndexForScan(tableName, indexName); err != api.DB_SUCCESS {
		t.Fatalf("create index: %v", err)
	}

	more := [][2]int32{{4, 10}, {5, 40}}
	if err := insertSecondaryIndexRows(tableName, more); err != api.DB_SUCCESS {
		t.Fatalf("insert more: %v", err)
	}

	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, trx, &crsr); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		t.Fatalf("CursorOpenTable: %v", err)
	}
	var idxCursor *api.Cursor
	if err := api.CursorOpenIndexUsingName(crsr, indexName, &idxCursor); err != api.DB_SUCCESS {
		_ = api.CursorClose(crsr)
		_ = api.TrxRollback(trx)
		t.Fatalf("CursorOpenIndexUsingName: %v", err)
	}
	if err := api.CursorFirst(idxCursor); err != api.DB_SUCCESS {
		if err != api.DB_RECORD_NOT_FOUND {
			_ = api.CursorClose(idxCursor)
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			t.Fatalf("CursorFirst: %v", err)
		}
	}
	prev := int32(-1)
	rowTpl := api.ClustReadTupleCreate(idxCursor)
	if rowTpl == nil {
		_ = api.CursorClose(idxCursor)
		_ = api.CursorClose(crsr)
		_ = api.TrxRollback(trx)
		t.Fatalf("ClustReadTupleCreate failed")
	}
	defer api.TupleDelete(rowTpl)

	for {
		err := api.CursorReadRow(idxCursor, rowTpl)
		if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
			break
		}
		if err != api.DB_SUCCESS {
			_ = api.CursorClose(idxCursor)
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			t.Fatalf("CursorReadRow: %v", err)
		}
		var c2 int32
		if err := api.TupleReadI32(rowTpl, 1, &c2); err != api.DB_SUCCESS {
			_ = api.CursorClose(idxCursor)
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			t.Fatalf("TupleReadI32: %v", err)
		}
		if prev != -1 && c2 < prev {
			_ = api.CursorClose(idxCursor)
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			t.Fatalf("secondary index order violated: %d < %d", c2, prev)
		}
		prev = c2
		if err := api.CursorNext(idxCursor); err != api.DB_SUCCESS {
			if err == api.DB_END_OF_INDEX {
				break
			}
			_ = api.CursorClose(idxCursor)
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			t.Fatalf("CursorNext: %v", err)
		}
	}

	_ = api.CursorClose(idxCursor)
	_ = api.CursorClose(crsr)
	if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
		t.Fatalf("TrxCommit: %v", err)
	}
	if err := api.TableDrop(nil, tableName); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop("test"); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createSecondaryIndexTable(tableName string) api.ErrCode {
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_INT, api.IB_COL_NONE, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c2", api.IB_INT, api.IB_COL_NONE, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	var idx *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "PRIMARY", &idx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "c1", 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaSetClustered(idx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaSetUnique(idx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableCreate(nil, schema, nil); err != api.DB_SUCCESS {
		api.TableSchemaDelete(schema)
		return err
	}
	api.TableSchemaDelete(schema)
	return api.DB_SUCCESS
}

func createSecondaryIndexForScan(tableName, indexName string) api.ErrCode {
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if err := api.SchemaLockExclusive(trx); err != api.DB_SUCCESS {
		return err
	}
	var idx *api.IndexSchema
	if err := api.IndexSchemaCreate(trx, indexName, tableName, &idx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "c2", 0); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.IndexCreate(idx, nil); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	api.IndexSchemaDelete(idx)
	return api.TrxCommit(trx)
}

func insertSecondaryIndexRows(tableName string, rows [][2]int32) api.ErrCode {
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, trx, &crsr); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.CursorLock(crsr, api.LockIX); err != api.DB_SUCCESS {
		_ = api.CursorClose(crsr)
		_ = api.TrxRollback(trx)
		return err
	}
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		_ = api.CursorClose(crsr)
		_ = api.TrxRollback(trx)
		return api.DB_ERROR
	}
	for _, row := range rows {
		if err := api.TupleWriteI32(tpl, 0, row[0]); err != api.DB_SUCCESS {
			api.TupleDelete(tpl)
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.TupleWriteI32(tpl, 1, row[1]); err != api.DB_SUCCESS {
			api.TupleDelete(tpl)
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.CursorInsertRow(crsr, tpl); err != api.DB_SUCCESS {
			api.TupleDelete(tpl)
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		tpl = api.TupleClear(tpl)
	}
	api.TupleDelete(tpl)
	_ = api.CursorClose(crsr)
	return api.TrxCommit(trx)
}
