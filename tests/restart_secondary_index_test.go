package tests

import (
	"fmt"
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

func TestRestartSecondaryIndexPersistence(t *testing.T) {
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
	if err := api.DatabaseCreate("restart_idx"); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	tableName := "restart_idx/t"
	if err := createRestartIndexTable(tableName); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}
	if err := insertRestartIndexRows(tableName); err != api.DB_SUCCESS {
		t.Fatalf("insert rows: %v", err)
	}
	indexName := fmt.Sprintf("%s_%s", tableName, "c2")
	if err := createSecondaryIndex(tableName, "c2", 0); err != api.DB_SUCCESS {
		t.Fatalf("create index: %v", err)
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

	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, trx, &crsr); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		t.Fatalf("CursorOpenTable: %v", err)
	}
	var idxCrsr *api.Cursor
	if err := api.CursorOpenIndexUsingName(crsr, indexName, &idxCrsr); err != api.DB_SUCCESS {
		_ = api.CursorClose(crsr)
		_ = api.TrxRollback(trx)
		t.Fatalf("CursorOpenIndexUsingName: %v", err)
	}
	if err := assertSecondaryIndexOrder(idxCrsr); err != api.DB_SUCCESS {
		_ = api.CursorClose(idxCrsr)
		_ = api.CursorClose(crsr)
		_ = api.TrxRollback(trx)
		t.Fatalf("assertSecondaryIndexOrder: %v", err)
	}
	_ = api.CursorClose(idxCrsr)
	_ = api.CursorClose(crsr)
	if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
		t.Fatalf("TrxCommit: %v", err)
	}

	if err := api.TableDrop(nil, tableName); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop("restart_idx"); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
	if err := api.Shutdown(api.ShutdownNormal); err != api.DB_SUCCESS {
		t.Fatalf("Shutdown final: %v", err)
	}
}

func createRestartIndexTable(tableName string) api.ErrCode {
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c2", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
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

func insertRestartIndexRows(tableName string) api.ErrCode {
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
	rows := [][2]uint32{{1, 30}, {2, 10}, {3, 20}, {4, 40}}
	for _, row := range rows {
		if err := api.TupleWriteU32(tpl, 0, row[0]); err != api.DB_SUCCESS {
			api.TupleDelete(tpl)
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.TupleWriteU32(tpl, 1, row[1]); err != api.DB_SUCCESS {
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

func assertSecondaryIndexOrder(crsr *api.Cursor) api.ErrCode {
	if err := api.CursorFirst(crsr); err != api.DB_SUCCESS {
		if err == api.DB_RECORD_NOT_FOUND {
			return api.DB_SUCCESS
		}
		return err
	}
	rowTpl := api.ClustReadTupleCreate(crsr)
	if rowTpl == nil {
		return api.DB_ERROR
	}
	defer api.TupleDelete(rowTpl)
	prev := int32(-1)
	for {
		err := api.CursorReadRow(crsr, rowTpl)
		if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
			return api.DB_SUCCESS
		}
		if err != api.DB_SUCCESS {
			return err
		}
		var c2 int32
		if err := api.TupleReadI32(rowTpl, 1, &c2); err != api.DB_SUCCESS {
			return err
		}
		if prev != -1 && c2 < prev {
			return api.DB_ERROR
		}
		prev = c2
		err = api.CursorNext(crsr)
		if err == api.DB_END_OF_INDEX {
			return api.DB_SUCCESS
		}
		if err != api.DB_SUCCESS {
			return err
		}
		rowTpl = api.TupleClear(rowTpl)
	}
}
