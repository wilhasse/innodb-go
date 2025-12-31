package tests

import (
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	typesDB    = "types_db"
	typesTable = "t"
)

func TestTypesHarness(t *testing.T) {
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
	if err := api.DatabaseCreate(typesDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	if err := createTypesTable(typesTableName()); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}

	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if trx == nil {
		t.Fatalf("TrxBegin returned nil")
	}
	var crsr *api.Cursor
	if err := api.CursorOpenTable(typesTableName(), trx, &crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorOpenTable: %v", err)
	}
	if err := api.CursorLock(crsr, api.LockIX); err != api.DB_SUCCESS {
		t.Fatalf("CursorLock: %v", err)
	}

	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		t.Fatalf("ClustReadTupleCreate returned nil")
	}
	if err := api.ColSetValue(tpl, 0, []byte("abcdefghij"), 10); err != api.DB_SUCCESS {
		t.Fatalf("set c1: %v", err)
	}
	if err := api.TupleWriteFloat(tpl, 2, 2.0); err != api.DB_SUCCESS {
		t.Fatalf("set c3: %v", err)
	}
	if err := api.TupleWriteDouble(tpl, 3, 3.0); err != api.DB_SUCCESS {
		t.Fatalf("set c4: %v", err)
	}
	if err := api.ColSetValue(tpl, 4, []byte("BLOB"), 4); err != api.DB_SUCCESS {
		t.Fatalf("set c5: %v", err)
	}
	if err := api.ColSetValue(tpl, 5, []byte("1.23"), 4); err != api.DB_SUCCESS {
		t.Fatalf("set c6: %v", err)
	}

	if err := api.CursorInsertRow(crsr, tpl); err != api.DB_DATA_MISMATCH {
		t.Fatalf("expected not-null violation, got %v", err)
	}

	if err := api.TupleWriteU32(tpl, 1, 1); err != api.DB_SUCCESS {
		t.Fatalf("set c2: %v", err)
	}
	if err := api.CursorInsertRow(crsr, tpl); err != api.DB_SUCCESS {
		t.Fatalf("insert row: %v", err)
	}
	api.TupleDelete(tpl)

	if err := api.CursorFirst(crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorFirst: %v", err)
	}

	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorClose: %v", err)
	}
	if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
		t.Fatalf("TrxCommit: %v", err)
	}
	if err := api.TableDrop(nil, typesTableName()); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(typesDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createTypesTable(tableName string) api.ErrCode {
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_VARCHAR, api.IB_COL_NONE, 0, 10); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c2", api.IB_INT, api.IB_COL_NOT_NULL, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c3", api.IB_FLOAT, api.IB_COL_NONE, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c4", api.IB_DOUBLE, api.IB_COL_NONE, 0, 8); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c5", api.IB_BLOB, api.IB_COL_NONE, 0, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c6", api.IB_DECIMAL, api.IB_COL_NONE, 0, 0); err != api.DB_SUCCESS {
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
	err := api.TableCreate(nil, schema, nil)
	api.TableSchemaDelete(schema)
	return err
}

func typesTableName() string {
	return typesDB + "/" + typesTable
}
