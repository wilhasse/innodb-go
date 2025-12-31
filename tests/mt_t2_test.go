package tests

import (
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	mtT2DB    = "test"
	mtT2Table = "mt_t2"
)

func TestMtT2Harness(t *testing.T) {
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
	if err := api.DatabaseCreate(mtT2DB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}

	tableName := mtT2DB + "/" + mtT2Table
	if err := createMtT2Table(tableName); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}
	if err := insertMtT2Rows(tableName); err != api.DB_SUCCESS {
		t.Fatalf("insert rows: %v", err)
	}
	if err := updateMtT2Row(tableName); err != api.DB_SUCCESS {
		t.Fatalf("update row: %v", err)
	}
	if err := deleteMtT2Row(tableName); err != api.DB_SUCCESS {
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
	if err := api.DatabaseDrop(mtT2DB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createMtT2Table(tableName string) api.ErrCode {
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
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
	if err := api.TableSchemaAddIndex(schema, "PK_index", &idx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "c1", 0); err != api.DB_SUCCESS {
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

func insertMtT2Rows(tableName string) api.ErrCode {
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
		id    uint32
		score uint32
	}{
		{1, 10},
		{2, 20},
	}
	for _, row := range rows {
		if err := api.TupleWriteU32(tpl, 0, row.id); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.TupleWriteU32(tpl, 1, row.score); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.TupleWriteU32(tpl, 2, 1); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.TupleWriteU32(tpl, 3, 0); err != api.DB_SUCCESS {
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

func updateMtT2Row(tableName string) api.ErrCode {
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
		var id uint32
		if err := api.TupleReadU32(oldTpl, 0, &id); err != api.DB_SUCCESS {
			return err
		}
		if id == 1 {
			if err := api.TupleCopy(newTpl, oldTpl); err != api.DB_SUCCESS {
				return err
			}
			var score uint32
			if err := api.TupleReadU32(oldTpl, 1, &score); err != api.DB_SUCCESS {
				return err
			}
			score += 100
			if err := api.TupleWriteU32(newTpl, 1, score); err != api.DB_SUCCESS {
				return err
			}
			if err := api.TupleWriteU32(newTpl, 3, 1); err != api.DB_SUCCESS {
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

func deleteMtT2Row(tableName string) api.ErrCode {
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
		var id uint32
		if err := api.TupleReadU32(tpl, 0, &id); err != api.DB_SUCCESS {
			return err
		}
		if id == 2 {
			return api.CursorDeleteRow(crsr)
		}
		if err := api.CursorNext(crsr); err != api.DB_SUCCESS {
			return err
		}
		tpl = api.TupleClear(tpl)
	}
}
