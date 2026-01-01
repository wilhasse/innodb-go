package api

import "testing"

func TestRollbackUndoRecordsRestoreState(t *testing.T) {
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
	if err := DatabaseCreate("undo_rb"); err != DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	var schema *TableSchema
	if err := TableSchemaCreate("undo_rb/t", &schema, IB_TBL_COMPACT, 0); err != DB_SUCCESS {
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

	baseTrx := TrxBegin(IB_TRX_REPEATABLE_READ)
	var baseCur *Cursor
	if err := CursorOpenTable("undo_rb/t", baseTrx, &baseCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable base: %v", err)
	}
	baseTpl := ClustReadTupleCreate(baseCur)
	if baseTpl == nil {
		t.Fatalf("ClustReadTupleCreate base returned nil")
	}
	if err := TupleWriteU32(baseTpl, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 base: %v", err)
	}
	if err := CursorInsertRow(baseCur, baseTpl); err != DB_SUCCESS {
		t.Fatalf("CursorInsertRow base: %v", err)
	}
	if err := TrxCommit(baseTrx); err != DB_SUCCESS {
		t.Fatalf("TrxCommit base: %v", err)
	}

	ibTrx := TrxBegin(IB_TRX_REPEATABLE_READ)
	var crsr *Cursor
	if err := CursorOpenTable("undo_rb/t", ibTrx, &crsr); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable: %v", err)
	}
	insertTpl := ClustReadTupleCreate(crsr)
	if insertTpl == nil {
		t.Fatalf("ClustReadTupleCreate insert returned nil")
	}
	if err := TupleWriteU32(insertTpl, 0, 2); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 insert: %v", err)
	}
	if err := CursorInsertRow(crsr, insertTpl); err != DB_SUCCESS {
		t.Fatalf("CursorInsertRow: %v", err)
	}

	updateOld := ClustSearchTupleCreate(crsr)
	if updateOld == nil {
		t.Fatalf("ClustSearchTupleCreate update old returned nil")
	}
	if err := TupleWriteU32(updateOld, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 update old: %v", err)
	}
	updateNew := ClustSearchTupleCreate(crsr)
	if updateNew == nil {
		t.Fatalf("ClustSearchTupleCreate update new returned nil")
	}
	if err := TupleWriteU32(updateNew, 0, 10); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 update new: %v", err)
	}
	if err := CursorUpdateRow(crsr, updateOld, updateNew); err != DB_SUCCESS {
		t.Fatalf("CursorUpdateRow: %v", err)
	}

	if err := CursorSetMatchMode(crsr, IB_EXACT_MATCH); err != DB_SUCCESS {
		t.Fatalf("CursorSetMatchMode: %v", err)
	}
	deleteTpl := ClustSearchTupleCreate(crsr)
	if deleteTpl == nil {
		t.Fatalf("ClustSearchTupleCreate delete returned nil")
	}
	if err := TupleWriteU32(deleteTpl, 0, 10); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 delete: %v", err)
	}
	var ret int
	if err := CursorMoveTo(crsr, deleteTpl, CursorGE, &ret); err != DB_SUCCESS {
		t.Fatalf("CursorMoveTo delete: %v", err)
	}
	if err := CursorDeleteRow(crsr); err != DB_SUCCESS {
		t.Fatalf("CursorDeleteRow: %v", err)
	}

	if err := TrxRollback(ibTrx); err != DB_SUCCESS {
		t.Fatalf("TrxRollback: %v", err)
	}

	var verify *Cursor
	if err := CursorOpenTable("undo_rb/t", nil, &verify); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable verify: %v", err)
	}
	if err := CursorSetMatchMode(verify, IB_EXACT_MATCH); err != DB_SUCCESS {
		t.Fatalf("CursorSetMatchMode verify: %v", err)
	}
	if ok, got := findRowValue(t, verify, 1); !ok || got != 1 {
		t.Fatalf("row 1 = %d ok=%v, want 1 true", got, ok)
	}
	if ok, _ := findRowValue(t, verify, 2); ok {
		t.Fatalf("row 2 still present after rollback")
	}
	if ok, _ := findRowValue(t, verify, 10); ok {
		t.Fatalf("row 10 still present after rollback")
	}
}

func findRowValue(t *testing.T, crsr *Cursor, value uint32) (bool, uint32) {
	t.Helper()
	if err := CursorReset(crsr); err != DB_SUCCESS {
		t.Fatalf("CursorReset: %v", err)
	}
	search := ClustSearchTupleCreate(crsr)
	if search == nil {
		t.Fatalf("ClustSearchTupleCreate search returned nil")
	}
	if err := TupleWriteU32(search, 0, value); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 search: %v", err)
	}
	var ret int
	if err := CursorMoveTo(crsr, search, CursorGE, &ret); err != DB_SUCCESS {
		if err == DB_RECORD_NOT_FOUND {
			return false, 0
		}
		t.Fatalf("CursorMoveTo search: %v", err)
	}
	readTpl := ClustReadTupleCreate(crsr)
	if readTpl == nil {
		t.Fatalf("ClustReadTupleCreate read returned nil")
	}
	if err := CursorReadRow(crsr, readTpl); err != DB_SUCCESS {
		t.Fatalf("CursorReadRow: %v", err)
	}
	var got uint32
	if err := TupleReadU32(readTpl, 0, &got); err != DB_SUCCESS {
		t.Fatalf("TupleReadU32: %v", err)
	}
	return true, got
}
