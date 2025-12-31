package trx

// UndoLogType identifies undo log kinds.
type UndoLogType int

const (
	UndoLogInsert UndoLogType = iota + 1
	UndoLogUpdate
)

// UndoLog stores undo records for a transaction.
type UndoLog struct {
	Type    UndoLogType
	TrxID   uint64
	Records []UndoRecord
}

// NewUndoLog creates a new undo log for a transaction.
func NewUndoLog(trxID uint64, typ UndoLogType) *UndoLog {
	return &UndoLog{Type: typ, TrxID: trxID}
}

// Append adds an undo record to the log.
func (log *UndoLog) Append(rec UndoRecord) {
	if log == nil {
		return
	}
	log.Records = append(log.Records, rec)
}

// Last returns the last record in the log.
func (log *UndoLog) Last() (UndoRecord, bool) {
	if log == nil || len(log.Records) == 0 {
		return UndoRecord{}, false
	}
	return log.Records[len(log.Records)-1], true
}

// Prev returns the record before the provided index.
func (log *UndoLog) Prev(index int) (UndoRecord, bool) {
	if log == nil || index <= 0 || index > len(log.Records)-1 {
		return UndoRecord{}, false
	}
	return log.Records[index-1], true
}

// Pop removes and returns the last record.
func (log *UndoLog) Pop() (UndoRecord, bool) {
	if log == nil || len(log.Records) == 0 {
		return UndoRecord{}, false
	}
	idx := len(log.Records) - 1
	rec := log.Records[idx]
	log.Records = log.Records[:idx]
	return rec, true
}

// Reset clears the log and updates the transaction id.
func (log *UndoLog) Reset(trxID uint64) {
	if log == nil {
		return
	}
	log.TrxID = trxID
	log.Records = nil
}
