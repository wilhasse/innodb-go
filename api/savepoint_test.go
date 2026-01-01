package api

import (
	"testing"

	"github.com/wilhasse/innodb-go/trx"
)

func TestSavepointRollbackUsesUndoRecords(t *testing.T) {
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
	if err := DatabaseCreate("savepoint_db"); err != DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	var schema *TableSchema
	if err := TableSchemaCreate("savepoint_db/t", &schema, IB_TBL_COMPACT, 0); err != DB_SUCCESS {
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

	ibTrx := TrxBegin(IB_TRX_REPEATABLE_READ)
	var crsr *Cursor
	if err := CursorOpenTable("savepoint_db/t", ibTrx, &crsr); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable: %v", err)
	}
	if err := CursorSetMatchMode(crsr, IB_EXACT_MATCH); err != DB_SUCCESS {
		t.Fatalf("CursorSetMatchMode: %v", err)
	}
	tpl := ClustReadTupleCreate(crsr)
	if tpl == nil {
		t.Fatalf("ClustReadTupleCreate returned nil")
	}
	if err := TupleWriteU32(tpl, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 c1: %v", err)
	}
	if err := TupleWriteU32(tpl, 1, 100); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 c2: %v", err)
	}
	if err := CursorInsertRow(crsr, tpl); err != DB_SUCCESS {
		t.Fatalf("CursorInsertRow: %v", err)
	}

	var savept *trx.Savepoint
	if err := SavepointTake(ibTrx, &savept); err != DB_SUCCESS {
		t.Fatalf("SavepointTake: %v", err)
	}
	if savept == nil {
		t.Fatalf("savepoint nil")
	}

	oldTpl := ClustSearchTupleCreate(crsr)
	if oldTpl == nil {
		t.Fatalf("ClustSearchTupleCreate old returned nil")
	}
	if err := TupleWriteU32(oldTpl, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 old: %v", err)
	}
	newTpl := ClustSearchTupleCreate(crsr)
	if newTpl == nil {
		t.Fatalf("ClustSearchTupleCreate new returned nil")
	}
	if err := TupleWriteU32(newTpl, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 new c1: %v", err)
	}
	if err := TupleWriteU32(newTpl, 1, 200); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 new c2: %v", err)
	}
	if err := CursorUpdateRow(crsr, oldTpl, newTpl); err != DB_SUCCESS {
		t.Fatalf("CursorUpdateRow: %v", err)
	}

	if err := SavepointRollback(ibTrx, savept); err != DB_SUCCESS {
		t.Fatalf("SavepointRollback: %v", err)
	}
	if len(ibTrx.UndoRecords) != savept.UndoRecLen {
		t.Fatalf("undo records=%d want %d", len(ibTrx.UndoRecords), savept.UndoRecLen)
	}
	if got := readValueByKey(t, crsr, 1); got != 100 {
		t.Fatalf("after rollback value=%d want 100", got)
	}
}
