package row

import "github.com/wilhasse/innodb-go/data"

// UndoOpType identifies undo log operations.
type UndoOpType int

const (
	UndoInsertOp UndoOpType = iota
	UndoModifyOp
)

// UndoEntry represents a logged undo operation.
type UndoEntry struct {
	Op     UndoOpType
	Tuple  *data.Tuple
	Before *data.Tuple
}

// UndoManager tracks mixed undo operations.
type UndoManager struct {
	Entries []UndoEntry
}

// RecordInsert logs an insert operation.
func (mgr *UndoManager) RecordInsert(tuple *data.Tuple) {
	if mgr == nil || tuple == nil {
		return
	}
	mgr.Entries = append(mgr.Entries, UndoEntry{Op: UndoInsertOp, Tuple: tuple})
}

// RecordModify logs a modify operation with a snapshot.
func (mgr *UndoManager) RecordModify(tuple *data.Tuple) {
	if mgr == nil || tuple == nil {
		return
	}
	mgr.Entries = append(mgr.Entries, UndoEntry{
		Op:     UndoModifyOp,
		Tuple:  tuple,
		Before: CopyRow(tuple, CopyData),
	})
}

// UndoLast reverts the most recent logged operation.
func (mgr *UndoManager) UndoLast(store *Store) error {
	if mgr == nil || len(mgr.Entries) == 0 {
		return ErrUndoEmpty
	}
	entry := mgr.Entries[len(mgr.Entries)-1]
	mgr.Entries = mgr.Entries[:len(mgr.Entries)-1]
	switch entry.Op {
	case UndoInsertOp:
		if store == nil || entry.Tuple == nil {
			return ErrUndoNotFound
		}
		if !removeRow(store, entry.Tuple) {
			return ErrUndoNotFound
		}
		return nil
	case UndoModifyOp:
		if entry.Tuple == nil || entry.Before == nil {
			return ErrUndoNotFound
		}
		restoreTuple(entry.Tuple, entry.Before)
		return nil
	default:
		return ErrUndoNotFound
	}
}
