package trx

// AppendUndoRecord appends an undo record to the transaction logs.
func AppendUndoRecord(trx *Trx, rec UndoRecord) {
	if trx == nil {
		return
	}
	if rec.UndoNo == 0 {
		trx.UndoNo++
		rec.UndoNo = trx.UndoNo
	} else if rec.UndoNo > trx.UndoNo {
		trx.UndoNo = rec.UndoNo
	}
	log := ensureUndoLog(trx, rec.Type)
	if log != nil {
		log.Append(rec)
	}
	trx.UndoRecords = append(trx.UndoRecords, rec)
}

func ensureUndoLog(trx *Trx, recType uint8) *UndoLog {
	if trx == nil {
		return nil
	}
	switch undoLogTypeForRecord(recType) {
	case UndoLogInsert:
		if trx.InsertUndo == nil {
			trx.InsertUndo = NewUndoLog(trx.ID, UndoLogInsert)
		}
		return trx.InsertUndo
	case UndoLogUpdate:
		if trx.UpdateUndo == nil {
			trx.UpdateUndo = NewUndoLog(trx.ID, UndoLogUpdate)
		}
		return trx.UpdateUndo
	default:
		return nil
	}
}

func undoLogTypeForRecord(recType uint8) UndoLogType {
	switch recType {
	case UndoInsertRec:
		return UndoLogInsert
	default:
		return UndoLogUpdate
	}
}
