package trx

import (
	"sync/atomic"
	"time"
)

// TrxCount tracks allocated transactions.
var TrxCount uint64

// TrxVarInit resets transaction globals.
func TrxVarInit() {
	atomic.StoreUint64(&TrxCount, 0)
}

// TrxCreate allocates a new transaction.
func TrxCreate() *Trx {
	atomic.AddUint64(&TrxCount, 1)
	return &Trx{State: TrxNotStarted}
}

// TrxBegin marks a transaction as active and assigns an id.
func TrxBegin(trx *Trx) {
	if trx == nil {
		return
	}
	if TrxSys == nil {
		TrxSysInit()
	}
	if trx.State == TrxActive {
		return
	}
	if trx.ID == 0 {
		trx.ID = TrxSysAllocID()
	}
	trx.State = TrxActive
	trx.StartTime = time.Now()
	TrxSysAddActive(trx)
}

// TrxCommit commits a transaction and clears its undo state.
func TrxCommit(trx *Trx) {
	if trx == nil || trx.State != TrxActive {
		return
	}
	TrxCloseReadView(trx)
	trx.State = TrxCommitted
	trx.UndoLog = nil
	trx.UndoRecords = nil
	trx.UndoNo = 0
	trx.InsertUndo = nil
	trx.UpdateUndo = nil
	trx.Savepoints = nil
	TrxSysRemoveActive(trx)
}

// TrxRollback rolls back the transaction using its undo log.
func TrxRollback(trx *Trx) {
	if trx == nil || trx.State != TrxActive {
		return
	}
	TrxCloseReadView(trx)
	Rollback(trx)
	trx.UndoRecords = nil
	trx.UndoNo = 0
	trx.InsertUndo = nil
	trx.UpdateUndo = nil
	trx.State = TrxRolledBack
	TrxSysRemoveActive(trx)
}

// TrxRelease decrements the transaction counter.
func TrxRelease(trx *Trx) {
	if trx == nil {
		return
	}
	if trx.State == TrxActive {
		TrxRollback(trx)
	}
	atomic.AddUint64(&TrxCount, ^uint64(0))
}
