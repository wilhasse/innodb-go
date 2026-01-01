package api

import (
	"testing"

	"github.com/wilhasse/innodb-go/trx"
)

func TestPurgeAfterViewsClosed(t *testing.T) {
	resetAPIState()
	if err := Init(); err != DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	defer func() {
		_ = Shutdown(ShutdownNormal)
	}()
	if err := Startup("barracuda"); err != DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}
	trx.PurgeVarInit()
	trx.PurgeSysCreate()
	trx.PurgeSys.PagesHandled = 0

	if err := DatabaseCreate("purge_db"); err != DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	var schema *TableSchema
	if err := TableSchemaCreate("purge_db/t", &schema, IB_TBL_COMPACT, 0); err != DB_SUCCESS {
		t.Fatalf("TableSchemaCreate: %v", err)
	}
	if err := TableSchemaAddCol(schema, "c1", IB_INT, IB_COL_UNSIGNED, 0, 4); err != DB_SUCCESS {
		t.Fatalf("TableSchemaAddCol c1: %v", err)
	}
	if err := TableSchemaAddCol(schema, "c2", IB_INT, IB_COL_UNSIGNED, 0, 4); err != DB_SUCCESS {
		t.Fatalf("TableSchemaAddCol c2: %v", err)
	}
	var idx *IndexSchema
	if err := TableSchemaAddIndex(schema, "PRIMARY", &idx); err != DB_SUCCESS {
		t.Fatalf("TableSchemaAddIndex: %v", err)
	}
	if err := IndexSchemaAddCol(idx, "c1", 0); err != DB_SUCCESS {
		t.Fatalf("IndexSchemaAddCol: %v", err)
	}
	if err := IndexSchemaSetClustered(idx); err != DB_SUCCESS {
		t.Fatalf("IndexSchemaSetClustered: %v", err)
	}
	if err := TableCreate(nil, schema, nil); err != DB_SUCCESS {
		t.Fatalf("TableCreate: %v", err)
	}

	baseTrx := TrxBegin(IB_TRX_REPEATABLE_READ)
	var baseCur *Cursor
	if err := CursorOpenTable("purge_db/t", baseTrx, &baseCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable base: %v", err)
	}
	baseTpl := ClustReadTupleCreate(baseCur)
	if baseTpl == nil {
		t.Fatalf("ClustReadTupleCreate base returned nil")
	}
	if err := TupleWriteU32(baseTpl, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 base c1: %v", err)
	}
	if err := TupleWriteU32(baseTpl, 1, 100); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 base c2: %v", err)
	}
	if err := CursorInsertRow(baseCur, baseTpl); err != DB_SUCCESS {
		t.Fatalf("CursorInsertRow base: %v", err)
	}
	if err := TrxCommit(baseTrx); err != DB_SUCCESS {
		t.Fatalf("TrxCommit base: %v", err)
	}

	reader := TrxBegin(IB_TRX_REPEATABLE_READ)
	var readCur *Cursor
	if err := CursorOpenTable("purge_db/t", reader, &readCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable reader: %v", err)
	}

	writer := TrxBegin(IB_TRX_REPEATABLE_READ)
	var writeCur *Cursor
	if err := CursorOpenTable("purge_db/t", writer, &writeCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable writer: %v", err)
	}
	oldTpl := ClustSearchTupleCreate(writeCur)
	if oldTpl == nil {
		t.Fatalf("ClustSearchTupleCreate old returned nil")
	}
	if err := TupleWriteU32(oldTpl, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 old: %v", err)
	}
	newTpl := ClustSearchTupleCreate(writeCur)
	if newTpl == nil {
		t.Fatalf("ClustSearchTupleCreate new returned nil")
	}
	if err := TupleWriteU32(newTpl, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 new c1: %v", err)
	}
	if err := TupleWriteU32(newTpl, 1, 200); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 new c2: %v", err)
	}
	if err := CursorUpdateRow(writeCur, oldTpl, newTpl); err != DB_SUCCESS {
		t.Fatalf("CursorUpdateRow: %v", err)
	}
	if err := TrxCommit(writer); err != DB_SUCCESS {
		t.Fatalf("TrxCommit writer: %v", err)
	}
	if trx.PurgeSys.PagesHandled != 0 {
		t.Fatalf("purge ran early: %d", trx.PurgeSys.PagesHandled)
	}
	if err := TrxRollback(reader); err != DB_SUCCESS {
		t.Fatalf("TrxRollback reader: %v", err)
	}
	if trx.PurgeSys.PagesHandled == 0 {
		t.Fatalf("expected purge after views closed")
	}
}
