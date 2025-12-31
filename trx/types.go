package trx

// UndoAction represents a rollback handler.
type UndoAction func()

// Trx holds transaction state for rollback.
type Trx struct {
	UndoLog    []UndoAction
	Savepoints []Savepoint
}

// Savepoint tracks the undo log position.
type Savepoint struct {
	UndoLen int
}
