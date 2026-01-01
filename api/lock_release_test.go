package api

import (
	"testing"
	"time"
)

func TestCommitReleasesLocks(t *testing.T) {
	tableName := setupLockTable(t, "lock_commit_db", 1)
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
	done := make(chan ErrCode, 1)
	go func() {
		done <- updateByKey(cur2, 1)
	}()
	time.Sleep(20 * time.Millisecond)

	if err := TrxCommit(trx1); err != DB_SUCCESS {
		t.Fatalf("TrxCommit trx1: %v", err)
	}
	if err := waitErr(t, done, time.Second); err != DB_SUCCESS {
		t.Fatalf("CursorUpdateRow after commit=%v, want DB_SUCCESS", err)
	}
	_ = TrxCommit(trx2)
}

func TestRollbackReleasesLocks(t *testing.T) {
	tableName := setupLockTable(t, "lock_rollback_db", 1)
	defer func() { _ = Shutdown(ShutdownNormal) }()

	trx1 := TrxBegin(IB_TRX_REPEATABLE_READ)
	var cur1 *Cursor
	if err := CursorOpenTable(tableName, trx1, &cur1); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable trx1: %v", err)
	}
	if err := CursorSetLockMode(cur1, LockIX); err != DB_SUCCESS {
		t.Fatalf("CursorSetLockMode: %v", err)
	}
	search := ClustSearchTupleCreate(cur1)
	if search == nil {
		t.Fatalf("ClustSearchTupleCreate search returned nil")
	}
	if err := TupleWriteU32(search, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 search: %v", err)
	}
	if err := CursorSetMatchMode(cur1, IB_EXACT_MATCH); err != DB_SUCCESS {
		t.Fatalf("CursorSetMatchMode: %v", err)
	}
	if err := CursorMoveTo(cur1, search, CursorGE, nil); err != DB_RECORD_NOT_FOUND {
		t.Fatalf("CursorMoveTo err=%v, want DB_RECORD_NOT_FOUND", err)
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
	if err := TrxRollback(trx1); err != DB_SUCCESS {
		t.Fatalf("TrxRollback trx1: %v", err)
	}
	if err := waitErr(t, done, time.Second); err != DB_SUCCESS {
		t.Fatalf("CursorInsertRow after rollback=%v, want DB_SUCCESS", err)
	}
	_ = TrxRollback(trx2)
}

func updateByKey(cur *Cursor, key uint32) ErrCode {
	if cur == nil {
		return DB_ERROR
	}
	oldTpl := ClustSearchTupleCreate(cur)
	if oldTpl == nil {
		return DB_ERROR
	}
	if err := TupleWriteU32(oldTpl, 0, key); err != DB_SUCCESS {
		return err
	}
	newTpl := ClustSearchTupleCreate(cur)
	if newTpl == nil {
		return DB_ERROR
	}
	if err := TupleWriteU32(newTpl, 0, key); err != DB_SUCCESS {
		return err
	}
	return CursorUpdateRow(cur, oldTpl, newTpl)
}
