package api

import "github.com/wilhasse/innodb-go/trx"

// SavepointTake creates a savepoint for a transaction.
func SavepointTake(ibTrx *trx.Trx, out **trx.Savepoint) ErrCode {
	if ibTrx == nil || out == nil {
		return DB_ERROR
	}
	savept := trx.SavepointTake(ibTrx)
	*out = &savept
	return DB_SUCCESS
}

// SavepointRollback rolls back a transaction to a savepoint.
func SavepointRollback(ibTrx *trx.Trx, savept *trx.Savepoint) ErrCode {
	if ibTrx == nil || savept == nil {
		return DB_ERROR
	}
	if !savepointExists(ibTrx, savept) {
		return DB_NO_SAVEPOINT
	}
	if err := rollbackUndoRecordsTo(ibTrx, savept.UndoRecLen); err != nil {
		return DB_ERROR
	}
	trx.RollbackToSavepoint(ibTrx, *savept)
	return DB_SUCCESS
}

// SavepointRelease removes a savepoint without rolling back.
func SavepointRelease(ibTrx *trx.Trx, savept *trx.Savepoint) ErrCode {
	if ibTrx == nil || savept == nil {
		return DB_ERROR
	}
	for i, sp := range ibTrx.Savepoints {
		if sp.UndoLen == savept.UndoLen && sp.UndoRecLen == savept.UndoRecLen {
			ibTrx.Savepoints = append(ibTrx.Savepoints[:i], ibTrx.Savepoints[i+1:]...)
			return DB_SUCCESS
		}
	}
	return DB_NO_SAVEPOINT
}

func savepointExists(ibTrx *trx.Trx, savept *trx.Savepoint) bool {
	if ibTrx == nil || savept == nil {
		return false
	}
	for _, sp := range ibTrx.Savepoints {
		if sp.UndoLen == savept.UndoLen && sp.UndoRecLen == savept.UndoRecLen {
			return true
		}
	}
	return false
}
