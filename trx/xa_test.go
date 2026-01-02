package trx

import "testing"

func TestXACommitTransitions(t *testing.T) {
	TrxSysVarInit()
	TrxVarInit()

	trx := TrxCreate()
	xid := XID{FormatID: 1, GTRID: []byte("gtrid"), BQUAL: []byte("bqual")}
	if !TrxXAStart(trx, xid) {
		t.Fatalf("TrxXAStart failed")
	}
	if trx.XAState != XAActive {
		t.Fatalf("XAState=%v, want %v", trx.XAState, XAActive)
	}
	if !TrxXAPrepare(trx) {
		t.Fatalf("TrxXAPrepare failed")
	}
	if trx.XAState != XAPrepared {
		t.Fatalf("XAState=%v, want %v", trx.XAState, XAPrepared)
	}
	if !TrxXACommit(trx) {
		t.Fatalf("TrxXACommit failed")
	}
	if trx.State != TrxCommitted {
		t.Fatalf("State=%v, want %v", trx.State, TrxCommitted)
	}
	if trx.XAState != XACommitted {
		t.Fatalf("XAState=%v, want %v", trx.XAState, XACommitted)
	}
}

func TestXARollbackTransitions(t *testing.T) {
	TrxSysVarInit()
	TrxVarInit()

	trx := TrxCreate()
	xid := XID{FormatID: 1, GTRID: []byte("gtrid"), BQUAL: []byte("bqual")}
	if !TrxXAStart(trx, xid) {
		t.Fatalf("TrxXAStart failed")
	}
	if !TrxXAPrepare(trx) {
		t.Fatalf("TrxXAPrepare failed")
	}
	if !TrxXARollback(trx) {
		t.Fatalf("TrxXARollback failed")
	}
	if trx.State != TrxRolledBack {
		t.Fatalf("State=%v, want %v", trx.State, TrxRolledBack)
	}
	if trx.XAState != XARolledBack {
		t.Fatalf("XAState=%v, want %v", trx.XAState, XARolledBack)
	}
}
