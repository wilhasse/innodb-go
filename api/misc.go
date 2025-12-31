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

// HandleErrors is a stub for error handling during query execution.
func HandleErrors(newErr *ErrCode, _ *trx.Trx, _ *que.Thr, _ *trx.Savepoint) Bool {
	if newErr != nil && *newErr == DB_SUCCESS {
		*newErr = DB_ERROR
	}
	return IBFalse
}

// TrxLockTableWithRetry is a stub for table locking with retry.
func TrxLockTableWithRetry(_ *trx.Trx, _ *dict.Table, _ lock.Mode) ErrCode {
	return DB_UNSUPPORTED
}

// UpdateStatisticsIfNeeded is a placeholder for statistics updates.
func UpdateStatisticsIfNeeded(_ *dict.Table) {}
