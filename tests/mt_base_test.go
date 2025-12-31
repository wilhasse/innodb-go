package tests

import (
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	mtBaseDB    = "mt_base"
	mtBaseTable = "t"
)

func TestMtBaseHarness(t *testing.T) {
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
	if err := api.DatabaseCreate(mtBaseDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}

	tableName := mtBaseDB + "/" + mtBaseTable
	if err := createMtBaseTable(tableName); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}
	if err := insertMtBaseRow(tableName); err != api.DB_SUCCESS {
		t.Fatalf("insert row: %v", err)
	}

	var tableID uint64
	if err := api.TableTruncate(tableName, &tableID); err != api.DB_SUCCESS {
		t.Fatalf("TableTruncate: %v", err)
	}
	if err := verifyMtBaseEmpty(tableName); err != api.DB_SUCCESS {
		t.Fatalf("verify empty: %v", err)
	}

	if err := api.TableDrop(nil, tableName); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(mtBaseDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createMtBaseTable(tableName string) api.ErrCode {
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
		return err
	}
	if err := api.TableCreate(trx, schema, nil); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	api.TableSchemaDelete(schema)
	return api.TrxCommit(trx)
}

func insertMtBaseRow(tableName string) api.ErrCode {
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
	if err := api.TupleWriteU32(tpl, 0, 1); err != api.DB_SUCCESS {
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
	api.TupleDelete(tpl)
	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	return api.TrxCommit(trx)
}

func verifyMtBaseEmpty(tableName string) api.ErrCode {
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, nil, &crsr); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CursorFirst(crsr); err != api.DB_RECORD_NOT_FOUND && err != api.DB_END_OF_INDEX {
		_ = api.CursorClose(crsr)
		return err
	}
	return api.CursorClose(crsr)
}
