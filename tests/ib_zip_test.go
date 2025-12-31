package tests

import (
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	zipDB       = "zip_db"
	zipTable    = "t"
	zipPageSize = 4
)

func TestZipHarness(t *testing.T) {
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
	if err := api.DatabaseCreate(zipDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	if err := createZipTable(zipTableName()); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}

	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if trx == nil {
		t.Fatalf("TrxBegin returned nil")
	}
	var crsr *api.Cursor
	if err := api.CursorOpenTable(zipTableName(), trx, &crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorOpenTable: %v", err)
	}
	if err := api.CursorLock(crsr, api.LockIX); err != api.DB_SUCCESS {
		t.Fatalf("CursorLock: %v", err)
	}
	if err := insertZipRow(crsr); err != api.DB_SUCCESS {
		t.Fatalf("insert row: %v", err)
	}
	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorClose: %v", err)
	}
	if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
		t.Fatalf("TrxCommit: %v", err)
	}
	if err := api.TableDrop(nil, zipTableName()); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(zipDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createZipTable(tableName string) api.ErrCode {
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if trx == nil {
		return api.DB_ERROR
	}
	if err := api.SchemaLockExclusive(trx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPRESSED, zipPageSize); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_VARCHAR, api.IB_COL_NONE, 0, 10); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c2", api.IB_INT, api.IB_COL_NONE, 0, 4); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	var idx *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "c1", &idx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "c1", 0); err != api.DB_SUCCESS {
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

func insertZipRow(crsr *api.Cursor) api.ErrCode {
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return api.DB_ERROR
	}
	defer api.TupleDelete(tpl)
	if err := api.ColSetValue(tpl, 0, []byte("x"), 1); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TupleWriteI32(tpl, 1, 1); err != api.DB_SUCCESS {
		return err
	}
	err := api.CursorInsertRow(crsr, tpl)
	if err == api.DB_DUPLICATE_KEY {
		return api.DB_SUCCESS
	}
	return err
}

func zipTableName() string {
	return zipDB + "/" + zipTable
}
