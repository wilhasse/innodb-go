package tests

import (
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

type crudRow struct {
	id  uint32
	val string
}

func TestCRUDPersistenceViaPageTree(t *testing.T) {
	resetAPI(t)
	dir := t.TempDir() + "/"

	const (
		dbName    = "crud_persist"
		tableName = dbName + "/t"
	)

	if err := api.Init(); err != api.DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	if err := api.CfgSet("data_home_dir", dir); err != api.DB_SUCCESS {
		t.Fatalf("CfgSet data_home_dir: %v", err)
	}
	if err := api.Startup("barracuda"); err != api.DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}
	if err := api.DatabaseCreate(dbName); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	if err := createCRUDTable(tableName); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}
	if err := insertCRUDRows(tableName, []crudRow{{1, "one"}, {2, "two"}}); err != api.DB_SUCCESS {
		t.Fatalf("insert rows: %v", err)
	}
	if err := updateCRUDRow(tableName, 2, "two2"); err != api.DB_SUCCESS {
		t.Fatalf("update row: %v", err)
	}
	if err := deleteCRUDRow(tableName, 1); err != api.DB_SUCCESS {
		t.Fatalf("delete row: %v", err)
	}
	if err := api.Shutdown(api.ShutdownNormal); err != api.DB_SUCCESS {
		t.Fatalf("Shutdown: %v", err)
	}

	if err := api.Init(); err != api.DB_SUCCESS {
		t.Fatalf("Init after restart: %v", err)
	}
	if err := api.CfgSet("data_home_dir", dir); err != api.DB_SUCCESS {
		t.Fatalf("CfgSet data_home_dir restart: %v", err)
	}
	if err := api.Startup("barracuda"); err != api.DB_SUCCESS {
		t.Fatalf("Startup restart: %v", err)
	}
	if err := api.DatabaseCreate(dbName); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate restart: %v", err)
	}

	rows, err := fetchCRUDRows(tableName)
	if err != api.DB_SUCCESS {
		t.Fatalf("fetch rows: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after restart, got %d", len(rows))
	}
	if got := rows[2]; got != "two2" {
		t.Fatalf("expected id=2 val=two2, got %q", got)
	}

	if err := api.TableDrop(nil, tableName); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(dbName); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
	if err := api.Shutdown(api.ShutdownNormal); err != api.DB_SUCCESS {
		t.Fatalf("Shutdown final: %v", err)
	}
}

func createCRUDTable(tableName string) api.ErrCode {
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "id", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "val", api.IB_VARCHAR, api.IB_COL_NONE, 0, 64); err != api.DB_SUCCESS {
		return err
	}
	var idx *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "PRIMARY", &idx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "id", 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaSetClustered(idx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaSetUnique(idx); err != api.DB_SUCCESS {
		return err
	}
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if trx == nil {
		return api.DB_ERROR
	}
	if err := api.SchemaLockExclusive(trx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	err := api.TableCreate(trx, schema, nil)
	api.TableSchemaDelete(schema)
	if err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	return api.TrxCommit(trx)
}

func insertCRUDRows(tableName string, rows []crudRow) api.ErrCode {
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
	for _, row := range rows {
		if err := api.TupleWriteU32(tpl, 0, row.id); err != api.DB_SUCCESS {
			api.TupleDelete(tpl)
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		valBytes := []byte(row.val)
		if err := api.ColSetValue(tpl, 1, valBytes, len(valBytes)); err != api.DB_SUCCESS {
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
	return api.TrxCommit(trx)
}

func updateCRUDRow(tableName string, id uint32, newVal string) api.ErrCode {
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
	if err := api.CursorFirst(crsr); err != api.DB_SUCCESS {
		_ = api.CursorClose(crsr)
		_ = api.TrxRollback(trx)
		if err == api.DB_RECORD_NOT_FOUND {
			return api.DB_SUCCESS
		}
		return err
	}
	oldTpl := api.ClustReadTupleCreate(crsr)
	newTpl := api.ClustReadTupleCreate(crsr)
	if oldTpl == nil || newTpl == nil {
		api.TupleDelete(oldTpl)
		api.TupleDelete(newTpl)
		_ = api.CursorClose(crsr)
		_ = api.TrxRollback(trx)
		return api.DB_ERROR
	}
	defer api.TupleDelete(oldTpl)
	defer api.TupleDelete(newTpl)

	for {
		if err := api.CursorReadRow(crsr, oldTpl); err != api.DB_SUCCESS {
			break
		}
		var got uint32
		if err := api.TupleReadU32(oldTpl, 0, &got); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if got == id {
			if err := api.TupleCopy(newTpl, oldTpl); err != api.DB_SUCCESS {
				_ = api.CursorClose(crsr)
				_ = api.TrxRollback(trx)
				return err
			}
			valBytes := []byte(newVal)
			if err := api.ColSetValue(newTpl, 1, valBytes, len(valBytes)); err != api.DB_SUCCESS {
				_ = api.CursorClose(crsr)
				_ = api.TrxRollback(trx)
				return err
			}
			if err := api.CursorUpdateRow(crsr, oldTpl, newTpl); err != api.DB_SUCCESS {
				_ = api.CursorClose(crsr)
				_ = api.TrxRollback(trx)
				return err
			}
			break
		}
		if err := api.CursorNext(crsr); err != api.DB_SUCCESS {
			if err == api.DB_END_OF_INDEX {
				break
			}
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		oldTpl = api.TupleClear(oldTpl)
		newTpl = api.TupleClear(newTpl)
	}
	_ = api.CursorClose(crsr)
	return api.TrxCommit(trx)
}

func deleteCRUDRow(tableName string, id uint32) api.ErrCode {
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
	if err := api.CursorFirst(crsr); err != api.DB_SUCCESS {
		_ = api.CursorClose(crsr)
		_ = api.TrxRollback(trx)
		if err == api.DB_RECORD_NOT_FOUND {
			return api.DB_SUCCESS
		}
		return err
	}
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		_ = api.CursorClose(crsr)
		_ = api.TrxRollback(trx)
		return api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	for {
		if err := api.CursorReadRow(crsr, tpl); err != api.DB_SUCCESS {
			break
		}
		var got uint32
		if err := api.TupleReadU32(tpl, 0, &got); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if got == id {
			if err := api.CursorDeleteRow(crsr); err != api.DB_SUCCESS {
				_ = api.CursorClose(crsr)
				_ = api.TrxRollback(trx)
				return err
			}
			break
		}
		if err := api.CursorNext(crsr); err != api.DB_SUCCESS {
			if err == api.DB_END_OF_INDEX {
				break
			}
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		tpl = api.TupleClear(tpl)
	}
	_ = api.CursorClose(crsr)
	return api.TrxCommit(trx)
}

func fetchCRUDRows(tableName string) (map[uint32]string, api.ErrCode) {
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, nil, &crsr); err != api.DB_SUCCESS {
		return nil, err
	}
	defer func() {
		_ = api.CursorClose(crsr)
	}()
	if err := api.CursorFirst(crsr); err != api.DB_SUCCESS {
		if err == api.DB_RECORD_NOT_FOUND {
			return map[uint32]string{}, api.DB_SUCCESS
		}
		return nil, err
	}
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return nil, api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	rows := make(map[uint32]string)
	for {
		if err := api.CursorReadRow(crsr, tpl); err != api.DB_SUCCESS {
			break
		}
		var id uint32
		if err := api.TupleReadU32(tpl, 0, &id); err != api.DB_SUCCESS {
			return nil, err
		}
		rows[id] = string(api.ColGetValue(tpl, 1))
		if err := api.CursorNext(crsr); err != api.DB_SUCCESS {
			if err == api.DB_END_OF_INDEX {
				break
			}
			return nil, err
		}
		tpl = api.TupleClear(tpl)
	}
	return rows, api.DB_SUCCESS
}
