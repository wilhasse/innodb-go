package tests

import (
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	btrSharedDB    = "btr_shared_db"
	btrSharedTable = "t"
)

func TestSharedBtreeAcrossCursors(t *testing.T) {
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
	if err := api.DatabaseCreate(btrSharedDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	tableName := btrSharedDB + "/" + btrSharedTable
	if err := createBtrSharedTable(tableName); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}

	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if trx == nil {
		t.Fatalf("TrxBegin returned nil")
	}
	var c1, c2 *api.Cursor
	if err := api.CursorOpenTable(tableName, trx, &c1); err != api.DB_SUCCESS {
		t.Fatalf("CursorOpenTable c1: %v", err)
	}
	if err := api.CursorOpenTable(tableName, trx, &c2); err != api.DB_SUCCESS {
		t.Fatalf("CursorOpenTable c2: %v", err)
	}

	tpl := api.ClustReadTupleCreate(c1)
	if tpl == nil {
		t.Fatalf("ClustReadTupleCreate returned nil")
	}
	if err := api.TupleWriteI32(tpl, 0, 42); err != api.DB_SUCCESS {
		t.Fatalf("TupleWriteI32: %v", err)
	}
	if err := api.CursorInsertRow(c1, tpl); err != api.DB_SUCCESS {
		t.Fatalf("CursorInsertRow: %v", err)
	}
	api.TupleDelete(tpl)

	if err := api.CursorFirst(c2); err != api.DB_SUCCESS {
		t.Fatalf("CursorFirst: %v", err)
	}
	readTpl := api.ClustReadTupleCreate(c2)
	if readTpl == nil {
		t.Fatalf("ClustReadTupleCreate returned nil")
	}
	if err := api.CursorReadRow(c2, readTpl); err != api.DB_SUCCESS {
		t.Fatalf("CursorReadRow: %v", err)
	}
	var got int32
	if err := api.TupleReadI32(readTpl, 0, &got); err != api.DB_SUCCESS {
		t.Fatalf("TupleReadI32: %v", err)
	}
	if got != 42 {
		t.Fatalf("shared cursor got=%d, want 42", got)
	}
	api.TupleDelete(readTpl)

	if err := api.CursorClose(c1); err != api.DB_SUCCESS {
		t.Fatalf("CursorClose c1: %v", err)
	}
	if err := api.CursorClose(c2); err != api.DB_SUCCESS {
		t.Fatalf("CursorClose c2: %v", err)
	}
	if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
		t.Fatalf("TrxCommit: %v", err)
	}
	if err := api.TableDrop(nil, tableName); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(btrSharedDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createBtrSharedTable(tableName string) api.ErrCode {
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_INT, api.IB_COL_NONE, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	var idx *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "c1", &idx); err != api.DB_SUCCESS {
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
