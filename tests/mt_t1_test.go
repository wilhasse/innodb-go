package tests

import (
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	mtT1DB    = "test"
	mtT1Table = "mt_t1"
)

func TestMtT1Harness(t *testing.T) {
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
	if err := api.DatabaseCreate(mtT1DB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}

	tableName := mtT1DB + "/" + mtT1Table
	if err := createMtT1Table(tableName); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}
	if err := insertMtT1Rows(tableName); err != api.DB_SUCCESS {
		t.Fatalf("insert rows: %v", err)
	}
	if err := updateMtT1Row(tableName); err != api.DB_SUCCESS {
		t.Fatalf("update row: %v", err)
	}
	if err := deleteMtT1Row(tableName); err != api.DB_SUCCESS {
		t.Fatalf("delete row: %v", err)
	}

	count, err := countRows(tableName)
	if err != api.DB_SUCCESS {
		t.Fatalf("count rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("row count=%d want=1", count)
	}

	if err := api.TableDrop(nil, tableName); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(mtT1DB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createMtT1Table(tableName string) api.ErrCode {
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "first", api.IB_VARCHAR, api.IB_COL_NONE, 0, 128); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "last", api.IB_VARCHAR, api.IB_COL_NONE, 0, 128); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "score", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "ins_run", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "upd_run", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	var idx *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "first_last", &idx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "first", 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "last", 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaSetClustered(idx); err != api.DB_SUCCESS {
		return err
	}
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if err := api.SchemaLockExclusive(trx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableCreate(trx, schema, nil); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	api.TableSchemaDelete(schema)
	return api.TrxCommit(trx)
}

func insertMtT1Rows(tableName string) api.ErrCode {
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, trx, &crsr); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		_ = api.CursorClose(crsr)
		_ = api.TrxRollback(trx)
		return api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	rows := []struct {
		first string
		last  string
		score uint32
	}{
		{"a", "alpha", 10},
		{"b", "beta", 20},
	}
	for _, row := range rows {
		if err := api.ColSetValue(tpl, 0, []byte(row.first), len(row.first)); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.ColSetValue(tpl, 1, []byte(row.last), len(row.last)); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.TupleWriteU32(tpl, 2, row.score); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.TupleWriteU32(tpl, 3, 1); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.TupleWriteU32(tpl, 4, 0); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.CursorInsertRow(crsr, tpl); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		tpl = api.TupleClear(tpl)
	}
	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	return api.TrxCommit(trx)
}

func updateMtT1Row(tableName string) api.ErrCode {
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, nil, &crsr); err != api.DB_SUCCESS {
		return err
	}
	defer api.CursorClose(crsr)
	if err := api.CursorFirst(crsr); err != api.DB_SUCCESS {
		return err
	}
	oldTpl := api.ClustReadTupleCreate(crsr)
	newTpl := api.ClustReadTupleCreate(crsr)
	if oldTpl == nil || newTpl == nil {
		return api.DB_ERROR
	}
	defer api.TupleDelete(oldTpl)
	defer api.TupleDelete(newTpl)

	for {
		if err := api.CursorReadRow(crsr, oldTpl); err != api.DB_SUCCESS {
			return err
		}
		first := api.ColGetValue(oldTpl, 0)
		if len(first) > 0 && string(first) == "a" {
			if err := api.TupleCopy(newTpl, oldTpl); err != api.DB_SUCCESS {
				return err
			}
			var score uint32
			if err := api.TupleReadU32(oldTpl, 2, &score); err != api.DB_SUCCESS {
				return err
			}
			score += 100
			if err := api.TupleWriteU32(newTpl, 2, score); err != api.DB_SUCCESS {
				return err
			}
			if err := api.TupleWriteU32(newTpl, 4, 1); err != api.DB_SUCCESS {
				return err
			}
			return api.CursorUpdateRow(crsr, oldTpl, newTpl)
		}
		if err := api.CursorNext(crsr); err != api.DB_SUCCESS {
			return err
		}
		oldTpl = api.TupleClear(oldTpl)
		newTpl = api.TupleClear(newTpl)
	}
}

func deleteMtT1Row(tableName string) api.ErrCode {
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, nil, &crsr); err != api.DB_SUCCESS {
		return err
	}
	defer api.CursorClose(crsr)
	if err := api.CursorFirst(crsr); err != api.DB_SUCCESS {
		return err
	}
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return api.DB_ERROR
	}
	defer api.TupleDelete(tpl)
	for {
		if err := api.CursorReadRow(crsr, tpl); err != api.DB_SUCCESS {
			return err
		}
		first := api.ColGetValue(tpl, 0)
		if len(first) > 0 && string(first) == "b" {
			return api.CursorDeleteRow(crsr)
		}
		if err := api.CursorNext(crsr); err != api.DB_SUCCESS {
			return err
		}
		tpl = api.TupleClear(tpl)
	}
}

func countRows(tableName string) (int, api.ErrCode) {
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, nil, &crsr); err != api.DB_SUCCESS {
		return 0, err
	}
	defer api.CursorClose(crsr)
	err := api.CursorFirst(crsr)
	if err == api.DB_RECORD_NOT_FOUND || err == api.DB_END_OF_INDEX {
		return 0, api.DB_SUCCESS
	}
	if err != api.DB_SUCCESS {
		return 0, err
	}
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return 0, api.DB_ERROR
	}
	defer api.TupleDelete(tpl)
	count := 0
	for {
		err = api.CursorReadRow(crsr, tpl)
		if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
			break
		}
		if err != api.DB_SUCCESS {
			return count, err
		}
		count++
		err = api.CursorNext(crsr)
		if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
			break
		}
		if err != api.DB_SUCCESS {
			return count, err
		}
		tpl = api.TupleClear(tpl)
	}
	return count, api.DB_SUCCESS
}
