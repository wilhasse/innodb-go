package tests

import (
	"fmt"
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	simpleBulkDB       = "simple_test"
	simpleBulkTable    = "data"
	simpleBulkRows     = 500
	simpleBulkBatch    = 100
	simpleBulkNameSize = 50
)

func TestSimpleBulkHarness(t *testing.T) {
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
	if err := api.DatabaseCreate(simpleBulkDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	if err := createSimpleBulkTable(simpleBulkTableName()); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}

	if err := simpleBulkInsert(simpleBulkRows, simpleBulkBatch); err != api.DB_SUCCESS {
		t.Fatalf("simple bulk insert: %v", err)
	}

	var crsr *api.Cursor
	if err := api.CursorOpenTable(simpleBulkTableName(), nil, &crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorOpenTable: %v", err)
	}
	count, err := countCursorRows(crsr)
	if err != api.DB_SUCCESS {
		t.Fatalf("count rows: %v", err)
	}
	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorClose: %v", err)
	}
	if count != simpleBulkRows {
		t.Fatalf("rows=%d, want %d", count, simpleBulkRows)
	}

	if err := api.TableDrop(nil, simpleBulkTableName()); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(simpleBulkDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createSimpleBulkTable(tableName string) api.ErrCode {
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if trx == nil {
		return api.DB_ERROR
	}
	if err := api.SchemaLockExclusive(trx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableSchemaAddCol(schema, "id", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableSchemaAddCol(schema, "name", api.IB_VARCHAR, api.IB_COL_NONE, 0, simpleBulkNameSize); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableSchemaAddCol(schema, "value", api.IB_INT, api.IB_COL_NONE, 0, 4); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	var idx *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "PRIMARY_KEY", &idx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "id", 0); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.IndexSchemaSetClustered(idx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	err := api.TableCreate(trx, schema, nil)
	api.TableSchemaDelete(schema)
	if err != api.DB_SUCCESS && err != api.DB_TABLE_IS_BEING_USED {
		_ = api.TrxRollback(trx)
		return err
	}
	return api.TrxCommit(trx)
}

func simpleBulkInsert(totalRows, batchSize int) api.ErrCode {
	tableName := simpleBulkTableName()
	for current := 1; current <= totalRows; {
		batchEnd := current + batchSize - 1
		if batchEnd > totalRows {
			batchEnd = totalRows
		}
		trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
		if trx == nil {
			return api.DB_ERROR
		}
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
		for i := current; i <= batchEnd; i++ {
			name := fmt.Sprintf("User_%d", i)
			if err := api.TupleWriteU32(tpl, 0, uint32(i)); err != api.DB_SUCCESS {
				api.TupleDelete(tpl)
				_ = api.CursorClose(crsr)
				_ = api.TrxRollback(trx)
				return err
			}
			if err := api.ColSetValue(tpl, 1, []byte(name), len(name)); err != api.DB_SUCCESS {
				api.TupleDelete(tpl)
				_ = api.CursorClose(crsr)
				_ = api.TrxRollback(trx)
				return err
			}
			if err := api.TupleWriteI32(tpl, 2, int32(i%1000)); err != api.DB_SUCCESS {
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
		if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
			return err
		}
		current = batchEnd + 1
	}
	return api.DB_SUCCESS
}

func countCursorRows(crsr *api.Cursor) (int, api.ErrCode) {
	if err := api.CursorFirst(crsr); err != api.DB_SUCCESS {
		if err == api.DB_RECORD_NOT_FOUND {
			return 0, api.DB_SUCCESS
		}
		return 0, err
	}
	count := 1
	for {
		err := api.CursorNext(crsr)
		if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
			return count, api.DB_SUCCESS
		}
		if err != api.DB_SUCCESS {
			return count, err
		}
		count++
	}
}

func simpleBulkTableName() string {
	return simpleBulkDB + "/" + simpleBulkTable
}
