package row

import (
	"errors"

	"github.com/wilhasse/innodb-go/data"
)

// ErrUndoEmpty is returned when undo log is empty.
var ErrUndoEmpty = errors.New("row: undo log empty")

// ErrUndoNotFound is returned when the target row is missing.
var ErrUndoNotFound = errors.New("row: undo row not found")

// UndoLog tracks insert operations for rollback.
type UndoLog struct {
	Inserts []*data.Tuple
}

// RecordInsert records a tuple insertion.
func (log *UndoLog) RecordInsert(tuple *data.Tuple) {
	if log == nil || tuple == nil {
		return
	}
	log.Inserts = append(log.Inserts, tuple)
}

// UndoLast rolls back the last recorded insert.
func (log *UndoLog) UndoLast(store *Store) error {
	if log == nil || len(log.Inserts) == 0 {
		return ErrUndoEmpty
	}
	if store == nil {
		return ErrUndoNotFound
	}
	last := log.Inserts[len(log.Inserts)-1]
	log.Inserts = log.Inserts[:len(log.Inserts)-1]
	if !removeRow(store, last) {
		return ErrUndoNotFound
	}
	return nil
}

func removeRow(store *Store, tuple *data.Tuple) bool {
	if store == nil || tuple == nil {
		return false
	}
	return store.RemoveTuple(tuple)
}
