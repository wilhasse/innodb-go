package tests

import (
	"fmt"
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	indexDB    = "test"
	indexTable = "t"
)

func TestIndexHarness(t *testing.T) {
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
	if err := api.DatabaseCreate(indexDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	if err := createIndexTable(); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}

	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	tableName := fmt.Sprintf("%s/%s", indexDB, indexTable)
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, trx, &crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorOpenTable: %v", err)
	}
	if err := api.CursorLock(crsr, api.LockIX); err != api.DB_SUCCESS {
		t.Fatalf("CursorLock: %v", err)
	}
	if err := insertIndexRows(crsr); err != api.DB_SUCCESS {
		t.Fatalf("insert rows: %v", err)
	}
	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorClose: %v", err)
	}
	if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
		t.Fatalf("TrxCommit: %v", err)
	}

	if err := api.TableDrop(nil, tableName); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(indexDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createIndexTable() api.ErrCode {
	tableName := fmt.Sprintf("%s/%s", indexDB, indexTable)
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_VARCHAR, api.IB_COL_NONE, 0, 10); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c2", api.IB_INT, api.IB_COL_NONE, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c3", api.IB_FLOAT, api.IB_COL_NONE, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c4", api.IB_DOUBLE, api.IB_COL_NONE, 0, 8); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c5", api.IB_DECIMAL, api.IB_COL_NONE, 0, 0); err != api.DB_SUCCESS {
		return err
	}

	var idx *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "c1", &idx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "c1", 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "c2", 2); err != api.DB_SCHEMA_ERROR {
		return api.DB_SCHEMA_ERROR
	}
	if err := api.IndexSchemaAddCol(idx, "c3", 2); err != api.DB_SCHEMA_ERROR {
		return api.DB_SCHEMA_ERROR
	}
	if err := api.IndexSchemaAddCol(idx, "c4", 2); err != api.DB_SCHEMA_ERROR {
		return api.DB_SCHEMA_ERROR
	}
	if err := api.IndexSchemaAddCol(idx, "c5", 2); err != api.DB_SCHEMA_ERROR {
		return api.DB_SCHEMA_ERROR
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

func insertIndexRows(crsr *api.Cursor) api.ErrCode {
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	if err := api.ColSetValue(tpl, 0, []byte("xxxxaaaa"), 8); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TupleWriteI32(tpl, 1, 2); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CursorInsertRow(crsr, tpl); err != api.DB_SUCCESS {
		return err
	}

	if err := api.ColSetValue(tpl, 0, []byte("xxxxbbbb"), 8); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TupleWriteI32(tpl, 1, 2); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CursorInsertRow(crsr, tpl); err != api.DB_DUPLICATE_KEY {
		return api.DB_DUPLICATE_KEY
	}

	return api.DB_SUCCESS
}
