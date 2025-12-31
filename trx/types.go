package trx

import "time"

// UndoAction represents a rollback handler.
type UndoAction func()

// TrxState tracks the lifecycle of a transaction.
type TrxState int

const (
	TrxNotStarted TrxState = iota
	TrxActive
	TrxCommitted
	TrxRolledBack
)

// Trx holds transaction state for rollback.
type Trx struct {
	ID         uint64
	State      TrxState
	StartTime  time.Time
	UndoLog    []UndoAction
	Savepoints []Savepoint
}

// Savepoint tracks the undo log position.
type Savepoint struct {
	UndoLen int
}
