package trx

import (
	"time"

	"github.com/wilhasse/innodb-go/read"
)

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
	ID          uint64
	State       TrxState
	XAState     XAState
	XID         *XID
	StartTime   time.Time
	ReadView    *read.ReadView
	UndoLog     []UndoAction
	UndoRecords []UndoRecord
	UndoNo      uint64
	InsertUndo  *UndoLog
	UpdateUndo  *UndoLog
	Savepoints  []Savepoint
}

// Savepoint tracks the undo log position.
type Savepoint struct {
	UndoLen    int
	UndoRecLen int
}
