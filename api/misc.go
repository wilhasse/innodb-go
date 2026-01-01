package api

import (
	stdos "os"

	"github.com/wilhasse/innodb-go/dict"
	"github.com/wilhasse/innodb-go/lock"
	"github.com/wilhasse/innodb-go/que"
	"github.com/wilhasse/innodb-go/trx"
)

// SesRollbackOnTimeout mirrors the C ses_rollback_on_timeout flag.
var SesRollbackOnTimeout Bool = IBFalse

// CreateTempFile creates a temporary file with the given prefix.
func CreateTempFile(prefix string) (*stdos.File, ErrCode) {
	file, err := stdos.CreateTemp("", prefix)
	if err != nil {
		return nil, DB_ERROR
	}
	return file, DB_SUCCESS
}

// TrxIsInterrupted reports whether a transaction was interrupted.
func TrxIsInterrupted(_ *trx.Trx) Bool {
	return IBFalse
}

// HandleErrors applies basic lock/deadlock handling and updates thread state.
func HandleErrors(newErr *ErrCode, ibTrx *trx.Trx, thr *que.Thr, savept *trx.Savepoint) Bool {
	if newErr == nil {
		return IBFalse
	}
	err := *newErr
	if err == DB_SUCCESS {
		return IBFalse
	}
	if thr != nil {
		switch err {
		case DB_LOCK_WAIT, DB_LOCK_WAIT_TIMEOUT:
			thr.State = que.ThrLockWait
			thr.LockState = que.LockRow
		default:
			thr.State = que.ThrError
		}
	}
	switch err {
	case DB_DEADLOCK:
		if ibTrx != nil {
			if savept != nil {
				_ = rollbackUndoRecordsTo(ibTrx, savept.UndoRecLen)
				trx.RollbackToSavepoint(ibTrx, *savept)
			} else {
				_ = TrxRollback(ibTrx)
			}
		}
		return IBTrue
	case DB_LOCK_WAIT_TIMEOUT:
		if SesRollbackOnTimeout == IBTrue && ibTrx != nil {
			if savept != nil {
				_ = rollbackUndoRecordsTo(ibTrx, savept.UndoRecLen)
				trx.RollbackToSavepoint(ibTrx, *savept)
			} else {
				_ = TrxRollback(ibTrx)
			}
			return IBTrue
		}
	}
	return IBFalse
}

// TrxLockTableWithRetry attempts a table lock and returns the lock status.
func TrxLockTableWithRetry(ibTrx *trx.Trx, table *dict.Table, mode lock.Mode) ErrCode {
	if ibTrx == nil || table == nil {
		return DB_ERROR
	}
	if table.Name == "" {
		return DB_INVALID_INPUT
	}
	_, status := lock.LockTable(ibTrx, table.Name, mode)
	return lockStatusToErr(status)
}

// UpdateStatisticsIfNeeded is a placeholder for statistics updates.
func UpdateStatisticsIfNeeded(_ *dict.Table) {}
