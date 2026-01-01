package api

import "github.com/wilhasse/innodb-go/trx"

// TrxIsolation mirrors ib_trx_level_t.
type TrxIsolation int

const (
	IB_TRX_REPEATABLE_READ TrxIsolation = iota
	IB_TRX_SERIALIZABLE
)

// TrxState mirrors ib_trx_state_t.
type TrxState int

const (
	IB_TRX_NOT_STARTED TrxState = iota
	IB_TRX_ACTIVE
	IB_TRX_COMMITTED
	IB_TRX_ROLLED_BACK
)

// TrxBegin starts a new transaction.
func TrxBegin(_ TrxIsolation) *trx.Trx {
	ibTrx := trx.TrxCreate()
	trx.TrxBegin(ibTrx)
	return ibTrx
}

// TrxCommit commits a transaction.
func TrxCommit(ibTrx *trx.Trx) ErrCode {
	if ibTrx == nil {
		return DB_ERROR
	}
	trx.TrxCommit(ibTrx)
	clearSchemaLock(ibTrx)
	return DB_SUCCESS
}

// TrxRollback rolls back a transaction.
func TrxRollback(ibTrx *trx.Trx) ErrCode {
	if ibTrx == nil {
		return DB_ERROR
	}
	undoErr := rollbackUndoRecords(ibTrx)
	trx.TrxRollback(ibTrx)
	clearSchemaLock(ibTrx)
	if undoErr != nil {
		return DB_ERROR
	}
	return DB_SUCCESS
}

// TrxStateGet returns the current transaction state.
func TrxStateGet(ibTrx *trx.Trx) TrxState {
	if ibTrx == nil {
		return IB_TRX_NOT_STARTED
	}
	switch ibTrx.State {
	case trx.TrxActive:
		return IB_TRX_ACTIVE
	case trx.TrxCommitted:
		return IB_TRX_COMMITTED
	case trx.TrxRolledBack:
		return IB_TRX_ROLLED_BACK
	default:
		return IB_TRX_NOT_STARTED
	}
}

// TrxRelease releases a transaction handle.
func TrxRelease(ibTrx *trx.Trx) ErrCode {
	if ibTrx == nil {
		return DB_ERROR
	}
	clearSchemaLock(ibTrx)
	trx.TrxRelease(ibTrx)
	return DB_SUCCESS
}
