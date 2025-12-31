package trx

import "testing"

func TestTransactionLifecycle(t *testing.T) {
	TrxVarInit()
	TrxSysVarInit()
	TrxSysInit()

	trx := TrxCreate()
	if trx.State != TrxNotStarted {
		t.Fatalf("state=%d", trx.State)
	}
	if TrxCount != 1 {
		t.Fatalf("count=%d", TrxCount)
	}

	undoCalls := 0
	RecordUndo(trx, func() { undoCalls++ })

	TrxBegin(trx)
	if trx.State != TrxActive || trx.ID == 0 {
		t.Fatalf("begin state=%d id=%d", trx.State, trx.ID)
	}
	if len(TrxSys.Active) != 1 {
		t.Fatalf("active=%d", len(TrxSys.Active))
	}

	TrxCommit(trx)
	if trx.State != TrxCommitted {
		t.Fatalf("commit state=%d", trx.State)
	}
	if len(trx.UndoLog) != 0 {
		t.Fatalf("undo len=%d", len(trx.UndoLog))
	}
	if len(TrxSys.Active) != 0 {
		t.Fatalf("active=%d", len(TrxSys.Active))
	}
	if undoCalls != 0 {
		t.Fatalf("undo calls=%d", undoCalls)
	}

	trx2 := TrxCreate()
	TrxBegin(trx2)
	RecordUndo(trx2, func() { undoCalls++ })
	TrxRollback(trx2)
	if trx2.State != TrxRolledBack {
		t.Fatalf("rollback state=%d", trx2.State)
	}
	if undoCalls != 1 {
		t.Fatalf("undo calls=%d", undoCalls)
	}
	if len(TrxSys.Active) != 0 {
		t.Fatalf("active=%d", len(TrxSys.Active))
	}

	TrxRelease(trx)
	TrxRelease(trx2)
	if TrxCount != 0 {
		t.Fatalf("count=%d", TrxCount)
	}
}
