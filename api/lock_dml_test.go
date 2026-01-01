package api

import (
	"testing"
	"time"
)

func setupLockTable(t *testing.T, dbName string, lockWaitTimeout uint64) string {
	t.Helper()
	resetAPIState()
	if err := Init(); err != DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	if err := CfgSet("lock_wait_timeout", lockWaitTimeout); err != DB_SUCCESS {
		t.Fatalf("CfgSet lock_wait_timeout: %v", err)
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
	tableName := setupLockTable(t, "lock_insert_db", 1)
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
	done := make(chan ErrCode, 1)
	go func() {
		done <- CursorInsertRow(cur2, tpl2)
	}()
	time.Sleep(20 * time.Millisecond)
	_ = TrxRollback(trx1)
	if err := waitErr(t, done, time.Second); err != DB_SUCCESS {
		t.Fatalf("CursorInsertRow trx2=%v, want DB_SUCCESS", err)
	}
	_ = TrxRollback(trx2)
}

func TestUpdateLockWait(t *testing.T) {
	tableName := setupLockTable(t, "lock_update_db", 1)
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
	if err := TrxCommit(trx1); err != DB_SUCCESS {
		t.Fatalf("TrxCommit trx1: %v", err)
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
	if err := CursorUpdateRow(cur2, oldTpl, newTpl); err != DB_SUCCESS {
		t.Fatalf("CursorUpdateRow trx2: %v", err)
	}

	trx3 := TrxBegin(IB_TRX_REPEATABLE_READ)
	var cur3 *Cursor
	if err := CursorOpenTable(tableName, trx3, &cur3); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable trx3: %v", err)
	}
	oldTpl2 := ClustSearchTupleCreate(cur3)
	if oldTpl2 == nil {
		t.Fatalf("ClustSearchTupleCreate old2 returned nil")
	}
	if err := TupleWriteU32(oldTpl2, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 old2: %v", err)
	}
	newTpl2 := ClustSearchTupleCreate(cur3)
	if newTpl2 == nil {
		t.Fatalf("ClustSearchTupleCreate new2 returned nil")
	}
	if err := TupleWriteU32(newTpl2, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 new2: %v", err)
	}
	done := make(chan ErrCode, 1)
	go func() {
		done <- CursorUpdateRow(cur3, oldTpl2, newTpl2)
	}()
	time.Sleep(20 * time.Millisecond)
	_ = TrxCommit(trx2)
	if err := waitErr(t, done, time.Second); err != DB_SUCCESS {
		t.Fatalf("CursorUpdateRow trx3=%v, want DB_SUCCESS", err)
	}
	_ = TrxRollback(trx3)
}

func TestDeleteLockWait(t *testing.T) {
	tableName := setupLockTable(t, "lock_delete_db", 1)
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
	if err := TrxCommit(trx1); err != DB_SUCCESS {
		t.Fatalf("TrxCommit trx1: %v", err)
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
	if err := CursorUpdateRow(cur2, oldTpl, newTpl); err != DB_SUCCESS {
		t.Fatalf("CursorUpdateRow trx2: %v", err)
	}

	trx3 := TrxBegin(IB_TRX_REPEATABLE_READ)
	var cur3 *Cursor
	if err := CursorOpenTable(tableName, trx3, &cur3); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable trx3: %v", err)
	}
	search2 := ClustSearchTupleCreate(cur3)
	if search2 == nil {
		t.Fatalf("ClustSearchTupleCreate search2 returned nil")
	}
	if err := TupleWriteU32(search2, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 search2: %v", err)
	}
	if err := CursorSetMatchMode(cur3, IB_EXACT_MATCH); err != DB_SUCCESS {
		t.Fatalf("CursorSetMatchMode: %v", err)
	}
	if err := CursorMoveTo(cur3, search2, CursorGE, nil); err != DB_SUCCESS {
		t.Fatalf("CursorMoveTo: %v", err)
	}
	done := make(chan ErrCode, 1)
	go func() {
		done <- CursorDeleteRow(cur3)
	}()
	time.Sleep(20 * time.Millisecond)
	_ = TrxCommit(trx2)
	if err := waitErr(t, done, time.Second); err != DB_SUCCESS {
		t.Fatalf("CursorDeleteRow trx3=%v, want DB_SUCCESS", err)
	}
	_ = TrxRollback(trx3)
}

func TestLockWaitTimeout(t *testing.T) {
	tableName := setupLockTable(t, "lock_timeout_db", 0)
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
	if err := CursorInsertRow(cur2, tpl2); err != DB_LOCK_WAIT_TIMEOUT {
		t.Fatalf("CursorInsertRow trx2=%v, want DB_LOCK_WAIT_TIMEOUT", err)
	}
	_ = TrxRollback(trx1)
	_ = TrxRollback(trx2)
}

func waitErr(t *testing.T, ch <-chan ErrCode, timeout time.Duration) ErrCode {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for lock operation")
		return DB_ERROR
	}
}
