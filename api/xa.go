package api

import (
	"github.com/wilhasse/innodb-go/lock"
	"github.com/wilhasse/innodb-go/trx"
)

// XID identifies a distributed transaction.
type XID = trx.XID

// XAState mirrors trx.XAState.
type XAState = trx.XAState

const (
	IB_XA_NOT_STARTED = trx.XANotStarted
	IB_XA_ACTIVE      = trx.XAActive
	IB_XA_PREPARED    = trx.XAPrepared
	IB_XA_COMMITTED   = trx.XACommitted
	IB_XA_ROLLED_BACK = trx.XARolledBack
)

// TrxXAStart associates an XA identifier with the transaction.
func TrxXAStart(ibTrx *trx.Trx, xid XID) ErrCode {
	if ibTrx == nil {
		return DB_ERROR
	}
	if !trx.TrxXAStart(ibTrx, xid) {
		return DB_ERROR
	}
	return DB_SUCCESS
}

// TrxXAPrepare prepares an XA transaction for 2PC.
func TrxXAPrepare(ibTrx *trx.Trx) ErrCode {
	if ibTrx == nil {
		return DB_ERROR
	}
	if !trx.TrxXAPrepare(ibTrx) {
		return DB_ERROR
	}
	return DB_SUCCESS
}

// TrxXACommit commits a prepared XA transaction.
func TrxXACommit(ibTrx *trx.Trx) ErrCode {
	if ibTrx == nil {
		return DB_ERROR
	}
	if !trx.TrxXACommit(ibTrx) {
		return DB_ERROR
	}
	lockReleaseAndPurge(ibTrx)
	return DB_SUCCESS
}

// TrxXARollback rolls back a prepared XA transaction.
func TrxXARollback(ibTrx *trx.Trx) ErrCode {
	if ibTrx == nil {
		return DB_ERROR
	}
	if !trx.TrxXARollback(ibTrx) {
		return DB_ERROR
	}
	lockReleaseAndPurge(ibTrx)
	return DB_SUCCESS
}

// TrxXAStateGet returns the XA state for a transaction.
func TrxXAStateGet(ibTrx *trx.Trx) XAState {
	if ibTrx == nil {
		return IB_XA_NOT_STARTED
	}
	return ibTrx.XAState
}

func lockReleaseAndPurge(ibTrx *trx.Trx) {
	lock.ReleaseAll(ibTrx)
	clearSchemaLock(ibTrx)
	purgeIfNeeded()
}
