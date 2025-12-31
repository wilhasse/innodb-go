package api

import "github.com/wilhasse/innodb-go/trx"

// TrxIsolation mirrors ib_trx_level_t.
type TrxIsolation int

const (
	IB_TRX_REPEATABLE_READ TrxIsolation = iota
	IB_TRX_SERIALIZABLE
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
	return DB_SUCCESS
}

// TrxRollback rolls back a transaction.
func TrxRollback(ibTrx *trx.Trx) ErrCode {
	if ibTrx == nil {
		return DB_ERROR
	}
	trx.TrxRollback(ibTrx)
	return DB_SUCCESS
}
