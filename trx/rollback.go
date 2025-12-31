package trx

// RecordUndo appends an undo action to the transaction log.
func RecordUndo(trx *Trx, action UndoAction) {
	if trx == nil || action == nil {
		return
	}
	trx.UndoLog = append(trx.UndoLog, action)
}

// SavepointTake creates a new savepoint for the transaction.
func SavepointTake(trx *Trx) Savepoint {
	if trx == nil {
		return Savepoint{}
	}
	savept := Savepoint{UndoLen: len(trx.UndoLog)}
	trx.Savepoints = append(trx.Savepoints, savept)
	return savept
}

// RollSavepointsFree removes savepoints after the provided savepoint.
func RollSavepointsFree(trx *Trx, savept *Savepoint) {
	if trx == nil {
		return
	}
	if savept == nil {
		trx.Savepoints = nil
		return
	}
	idx := -1
	for i := len(trx.Savepoints) - 1; i >= 0; i-- {
		if trx.Savepoints[i].UndoLen == savept.UndoLen {
			idx = i
			break
		}
	}
	if idx >= 0 {
		trx.Savepoints = trx.Savepoints[:idx+1]
	}
}

// GeneralRollback rolls back all or part of a transaction.
func GeneralRollback(trx *Trx, partial bool, savept *Savepoint) int {
	if trx == nil {
		return 0
	}
	target := 0
	if partial && savept != nil {
		target = savept.UndoLen
	}
	if target < 0 {
		target = 0
	}
	if target > len(trx.UndoLog) {
		target = len(trx.UndoLog)
	}
	rolled := 0
	for i := len(trx.UndoLog) - 1; i >= target; i-- {
		if trx.UndoLog[i] != nil {
			trx.UndoLog[i]()
		}
		rolled++
	}
	trx.UndoLog = trx.UndoLog[:target]
	if partial {
		RollSavepointsFree(trx, savept)
	} else {
		RollSavepointsFree(trx, nil)
	}
	return rolled
}

// Rollback rolls back the entire transaction.
func Rollback(trx *Trx) int {
	return GeneralRollback(trx, false, nil)
}

// RollbackToSavepoint rolls back to the provided savepoint.
func RollbackToSavepoint(trx *Trx, savept Savepoint) int {
	return GeneralRollback(trx, true, &savept)
}
