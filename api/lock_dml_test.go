package api

import "testing"

func setupLockTable(t *testing.T, dbName string) string {
	t.Helper()
	resetAPIState()
	if err := Init(); err != DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	if err := Startup("barracuda"); err != DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}
	if err := DatabaseCreate(dbName); err != DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	var schema *TableSchema
	tableName := dbName + "/t"
	if err := TableSchemaCreate(tableName, &schema, IB_TBL_COMPACT, 0); err != DB_SUCCESS {
		t.Fatalf("TableSchemaCreate: %v", err)
	}
	if err := TableSchemaAddCol(schema, "c1", IB_INT, IB_COL_UNSIGNED, 0, 4); err != DB_SUCCESS {
		t.Fatalf("TableSchemaAddCol c1: %v", err)
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
	return tableName
}

func TestInsertLockWait(t *testing.T) {
	tableName := setupLockTable(t, "lock_insert_db")
	defer func() { _ = Shutdown(ShutdownNormal) }()

	trx1 := TrxBegin(IB_TRX_REPEATABLE_READ)
	var cur1 *Cursor
	if err := CursorOpenTable(tableName, trx1, &cur1); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable trx1: %v", err)
	}
	tpl1 := ClustReadTupleCreate(cur1)
	if tpl1 == nil {
		t.Fatalf("ClustReadTupleCreate trx1 returned nil")
	}
	if err := TupleWriteU32(tpl1, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 trx1: %v", err)
	}
	if err := CursorInsertRow(cur1, tpl1); err != DB_SUCCESS {
		t.Fatalf("CursorInsertRow trx1: %v", err)
	}

	trx2 := TrxBegin(IB_TRX_REPEATABLE_READ)
	var cur2 *Cursor
	if err := CursorOpenTable(tableName, trx2, &cur2); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable trx2: %v", err)
	}
	tpl2 := ClustReadTupleCreate(cur2)
	if tpl2 == nil {
		t.Fatalf("ClustReadTupleCreate trx2 returned nil")
	}
	if err := TupleWriteU32(tpl2, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 trx2: %v", err)
	}
	if err := CursorInsertRow(cur2, tpl2); err != DB_LOCK_WAIT {
		t.Fatalf("CursorInsertRow trx2=%v, want DB_LOCK_WAIT", err)
	}
	_ = TrxRollback(trx1)
	_ = TrxRollback(trx2)
}

func TestUpdateLockWait(t *testing.T) {
	tableName := setupLockTable(t, "lock_update_db")
	defer func() { _ = Shutdown(ShutdownNormal) }()

	trx1 := TrxBegin(IB_TRX_REPEATABLE_READ)
	var cur1 *Cursor
	if err := CursorOpenTable(tableName, trx1, &cur1); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable trx1: %v", err)
	}
	tpl1 := ClustReadTupleCreate(cur1)
	if tpl1 == nil {
		t.Fatalf("ClustReadTupleCreate trx1 returned nil")
	}
	if err := TupleWriteU32(tpl1, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 trx1: %v", err)
	}
	if err := CursorInsertRow(cur1, tpl1); err != DB_SUCCESS {
		t.Fatalf("CursorInsertRow trx1: %v", err)
	}

	trx2 := TrxBegin(IB_TRX_REPEATABLE_READ)
	var cur2 *Cursor
	if err := CursorOpenTable(tableName, trx2, &cur2); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable trx2: %v", err)
	}
	oldTpl := ClustSearchTupleCreate(cur2)
	if oldTpl == nil {
		t.Fatalf("ClustSearchTupleCreate old returned nil")
	}
	if err := TupleWriteU32(oldTpl, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 old: %v", err)
	}
	newTpl := ClustSearchTupleCreate(cur2)
	if newTpl == nil {
		t.Fatalf("ClustSearchTupleCreate new returned nil")
	}
	if err := TupleWriteU32(newTpl, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 new: %v", err)
	}
	if err := CursorUpdateRow(cur2, oldTpl, newTpl); err != DB_LOCK_WAIT {
		t.Fatalf("CursorUpdateRow trx2=%v, want DB_LOCK_WAIT", err)
	}
	_ = TrxRollback(trx1)
	_ = TrxRollback(trx2)
}

func TestDeleteLockWait(t *testing.T) {
	tableName := setupLockTable(t, "lock_delete_db")
	defer func() { _ = Shutdown(ShutdownNormal) }()

	trx1 := TrxBegin(IB_TRX_REPEATABLE_READ)
	var cur1 *Cursor
	if err := CursorOpenTable(tableName, trx1, &cur1); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable trx1: %v", err)
	}
	tpl1 := ClustReadTupleCreate(cur1)
	if tpl1 == nil {
		t.Fatalf("ClustReadTupleCreate trx1 returned nil")
	}
	if err := TupleWriteU32(tpl1, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 trx1: %v", err)
	}
	if err := CursorInsertRow(cur1, tpl1); err != DB_SUCCESS {
		t.Fatalf("CursorInsertRow trx1: %v", err)
	}

	trx2 := TrxBegin(IB_TRX_REPEATABLE_READ)
	var cur2 *Cursor
	if err := CursorOpenTable(tableName, trx2, &cur2); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable trx2: %v", err)
	}
	search := ClustSearchTupleCreate(cur2)
	if search == nil {
		t.Fatalf("ClustSearchTupleCreate search returned nil")
	}
	if err := TupleWriteU32(search, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 search: %v", err)
	}
	if err := CursorSetMatchMode(cur2, IB_EXACT_MATCH); err != DB_SUCCESS {
		t.Fatalf("CursorSetMatchMode: %v", err)
	}
	if err := CursorMoveTo(cur2, search, CursorGE, nil); err != DB_SUCCESS {
		t.Fatalf("CursorMoveTo: %v", err)
	}
	if err := CursorDeleteRow(cur2); err != DB_LOCK_WAIT {
		t.Fatalf("CursorDeleteRow trx2=%v, want DB_LOCK_WAIT", err)
	}
	_ = TrxRollback(trx1)
	_ = TrxRollback(trx2)
}
