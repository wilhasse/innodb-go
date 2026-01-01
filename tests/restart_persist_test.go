package tests

import (
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	restartDB    = "restart_test"
	restartTable = "t"
)

func TestRestartPersistence(t *testing.T) {
	resetAPI(t)
	dir := t.TempDir() + "/"

	if err := api.Init(); err != api.DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	if err := api.CfgSet("data_home_dir", dir); err != api.DB_SUCCESS {
		t.Fatalf("CfgSet data_home_dir: %v", err)
	}
	if err := api.Startup("barracuda"); err != api.DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}
	if err := api.DatabaseCreate(restartDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}

	tableName := restartDB + "/" + restartTable
	if err := createRestartTable(tableName); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}
	if err := insertRestartRows(tableName, []uint32{1, 2, 3}); err != api.DB_SUCCESS {
		t.Fatalf("insert rows: %v", err)
	}

	if err := api.Shutdown(api.ShutdownNormal); err != api.DB_SUCCESS {
		t.Fatalf("Shutdown: %v", err)
	}

	if err := api.Init(); err != api.DB_SUCCESS {
		t.Fatalf("Init after restart: %v", err)
	}
	if err := api.CfgSet("data_home_dir", dir); err != api.DB_SUCCESS {
		t.Fatalf("CfgSet data_home_dir after restart: %v", err)
	}
	if err := api.Startup("barracuda"); err != api.DB_SUCCESS {
		t.Fatalf("Startup after restart: %v", err)
	}
	if err := api.DatabaseCreate(restartDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate after restart: %v", err)
	}
	if err := createRestartTable(tableName); err != api.DB_SUCCESS && err != api.DB_TABLE_IS_BEING_USED {
		t.Fatalf("recreate table: %v", err)
	}
	if err := verifyRestartRows(tableName, []uint32{1, 2, 3}); err != api.DB_SUCCESS {
		t.Fatalf("verify rows: %v", err)
	}

	if err := api.TableDrop(nil, tableName); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(restartDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
	if err := api.Shutdown(api.ShutdownNormal); err != api.DB_SUCCESS {
		t.Fatalf("Shutdown final: %v", err)
	}
}

func createRestartTable(tableName string) api.ErrCode {
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
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
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
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

func insertRestartRows(tableName string, values []uint32) api.ErrCode {
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
	for _, val := range values {
		if err := api.TupleWriteU32(tpl, 0, val); err != api.DB_SUCCESS {
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

func verifyRestartRows(tableName string, expected []uint32) api.ErrCode {
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, nil, &crsr); err != api.DB_SUCCESS {
		return err
	}
	defer func() {
		_ = api.CursorClose(crsr)
	}()
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	if err := api.CursorFirst(crsr); err == api.DB_END_OF_INDEX {
		return api.DB_ERROR
	} else if err != api.DB_SUCCESS {
		return err
	}

	got := make([]uint32, 0)
	for {
		if err := api.CursorReadRow(crsr, tpl); err != api.DB_SUCCESS {
			return err
		}
		var val uint32
		if err := api.TupleReadU32(tpl, 0, &val); err != api.DB_SUCCESS {
			return err
		}
		got = append(got, val)
		if err := api.CursorNext(crsr); err == api.DB_END_OF_INDEX {
			break
		} else if err != api.DB_SUCCESS {
			return err
		}
	}
	if len(got) != len(expected) {
		return api.DB_ERROR
	}
	for i, val := range expected {
		if got[i] != val {
			return api.DB_ERROR
		}
	}
	return api.DB_SUCCESS
}
