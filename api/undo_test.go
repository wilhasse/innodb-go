package api

import (
	"testing"

	"github.com/wilhasse/innodb-go/trx"
)

func TestUndoRecordsRecordedOnWriteOps(t *testing.T) {
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
	if err := DatabaseCreate("undo_db"); err != DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	var schema *TableSchema
	if err := TableSchemaCreate("undo_db/t", &schema, IB_TBL_COMPACT, 0); err != DB_SUCCESS {
		t.Fatalf("TableSchemaCreate: %v", err)
	}
	if err := TableSchemaAddCol(schema, "c1", IB_INT, IB_COL_UNSIGNED, 0, 4); err != DB_SUCCESS {
		t.Fatalf("TableSchemaAddCol: %v", err)
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
	if err := CursorOpenTable("undo_db/t", ibTrx, &crsr); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable: %v", err)
	}
	tpl := ClustReadTupleCreate(crsr)
	if tpl == nil {
		t.Fatalf("ClustReadTupleCreate returned nil")
	}
	if err := TupleWriteU32(tpl, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32: %v", err)
	}
	if err := CursorInsertRow(crsr, tpl); err != DB_SUCCESS {
		t.Fatalf("CursorInsertRow: %v", err)
	}

	updateOld := ClustSearchTupleCreate(crsr)
	if updateOld == nil {
		t.Fatalf("ClustSearchTupleCreate returned nil")
	}
	if err := TupleWriteU32(updateOld, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 old: %v", err)
	}
	updateNew := ClustSearchTupleCreate(crsr)
	if updateNew == nil {
		t.Fatalf("ClustSearchTupleCreate returned nil")
	}
	if err := TupleWriteU32(updateNew, 0, 2); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 new: %v", err)
	}
	if err := CursorUpdateRow(crsr, updateOld, updateNew); err != DB_SUCCESS {
		t.Fatalf("CursorUpdateRow: %v", err)
	}
	if err := CursorFirst(crsr); err != DB_SUCCESS {
		t.Fatalf("CursorFirst: %v", err)
	}
	if err := CursorDeleteRow(crsr); err != DB_SUCCESS {
		t.Fatalf("CursorDeleteRow: %v", err)
	}

	if len(ibTrx.UndoRecords) != 3 {
		t.Fatalf("undo records=%d, want 3", len(ibTrx.UndoRecords))
	}
	if ibTrx.InsertUndo == nil || ibTrx.UpdateUndo == nil {
		t.Fatalf("expected insert/update undo logs")
	}
	if ibTrx.InsertUndo.Type != trx.UndoLogInsert || ibTrx.UpdateUndo.Type != trx.UndoLogUpdate {
		t.Fatalf("unexpected undo log types")
	}
}
