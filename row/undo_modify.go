package row

import "github.com/wilhasse/innodb-go/data"

// UndoModifyEntry stores a snapshot for a modified row.
type UndoModifyEntry struct {
	Tuple  *data.Tuple
	Before *data.Tuple
}

// UndoModifyLog tracks row modifications for rollback.
type UndoModifyLog struct {
	Entries []UndoModifyEntry
}

// RecordModify records a snapshot before modification.
func (log *UndoModifyLog) RecordModify(tuple *data.Tuple) {
	if log == nil || tuple == nil {
		return
	}
	log.Entries = append(log.Entries, UndoModifyEntry{
		Tuple:  tuple,
		Before: CopyRow(tuple, CopyData),
	})
}

// UndoLast restores the last modified row.
func (log *UndoModifyLog) UndoLast() error {
	if log == nil || len(log.Entries) == 0 {
		return ErrUndoEmpty
	}
	entry := log.Entries[len(log.Entries)-1]
	log.Entries = log.Entries[:len(log.Entries)-1]
	if entry.Tuple == nil || entry.Before == nil {
		return ErrUndoNotFound
	}
	restoreTuple(entry.Tuple, entry.Before)
	return nil
}

func restoreTuple(dst, src *data.Tuple) {
	if dst == nil || src == nil {
		return
	}
	copyTuple := CopyRow(src, CopyData)
	if copyTuple == nil {
		return
	}
	*dst = *copyTuple
}
