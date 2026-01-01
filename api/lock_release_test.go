package api

import "testing"

func TestCommitReleasesLocks(t *testing.T) {
	tableName := setupLockTable(t, "lock_commit_db")
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
	if err := updateByKey(cur2, 1); err != DB_LOCK_WAIT {
		t.Fatalf("CursorUpdateRow trx2=%v, want DB_LOCK_WAIT", err)
	}

	if err := TrxCommit(trx1); err != DB_SUCCESS {
		t.Fatalf("TrxCommit trx1: %v", err)
	}
	if err := updateByKey(cur2, 1); err != DB_SUCCESS {
		t.Fatalf("CursorUpdateRow after commit=%v, want DB_SUCCESS", err)
	}
	_ = TrxCommit(trx2)
}

func TestRollbackReleasesLocks(t *testing.T) {
	tableName := setupLockTable(t, "lock_rollback_db")
	defer func() { _ = Shutdown(ShutdownNormal) }()

	base := TrxBegin(IB_TRX_REPEATABLE_READ)
	var baseCur *Cursor
	if err := CursorOpenTable(tableName, base, &baseCur); err != DB_SUCCESS {
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
	if err := TrxCommit(base); err != DB_SUCCESS {
		t.Fatalf("TrxCommit base: %v", err)
	}

	trx1 := TrxBegin(IB_TRX_REPEATABLE_READ)
	var cur1 *Cursor
	if err := CursorOpenTable(tableName, trx1, &cur1); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable trx1: %v", err)
	}
	if err := updateByKey(cur1, 1); err != DB_SUCCESS {
		t.Fatalf("CursorUpdateRow trx1: %v", err)
	}

	trx2 := TrxBegin(IB_TRX_REPEATABLE_READ)
	var cur2 *Cursor
	if err := CursorOpenTable(tableName, trx2, &cur2); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable trx2: %v", err)
	}
	if err := updateByKey(cur2, 1); err != DB_LOCK_WAIT {
		t.Fatalf("CursorUpdateRow trx2=%v, want DB_LOCK_WAIT", err)
	}
	if err := TrxRollback(trx1); err != DB_SUCCESS {
		t.Fatalf("TrxRollback trx1: %v", err)
	}
	if err := updateByKey(cur2, 1); err != DB_SUCCESS {
		t.Fatalf("CursorUpdateRow after rollback=%v, want DB_SUCCESS", err)
	}
	_ = TrxCommit(trx2)
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
