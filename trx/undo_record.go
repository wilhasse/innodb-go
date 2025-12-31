package trx

import (
	"encoding/binary"
	"errors"
)

const (
	// UndoInsertRec mirrors TRX_UNDO_INSERT_REC.
	UndoInsertRec = 11
	// UndoUpdExistRec mirrors TRX_UNDO_UPD_EXIST_REC.
	UndoUpdExistRec = 12
	// UndoUpdDelRec mirrors TRX_UNDO_UPD_DEL_REC.
	UndoUpdDelRec = 13
	// UndoDelMarkRec mirrors TRX_UNDO_DEL_MARK_REC.
	UndoDelMarkRec = 14
)

const (
	undoHeaderTypeOffset      = 0
	undoHeaderCmplInfoOffset  = 1
	undoHeaderExternOffset    = 2
	undoHeaderUndoNoOffset    = 3
	undoHeaderTableIDOffset   = 11
	UndoRecordHeaderSize      = 19
	undoRecordMinEncodedBytes = UndoRecordHeaderSize
)

// ErrUndoRecordTooShort reports a truncated undo record buffer.
var ErrUndoRecordTooShort = errors.New("trx: undo record too short")

// UndoRecord represents an encoded undo record header plus payload.
type UndoRecord struct {
	Type          uint8
	CmplInfo      uint8
	UpdatedExtern bool
	UndoNo        uint64
	TableID       uint64
	Data          []byte
}

// EncodeUndoRecord serializes an undo record into a byte slice.
func EncodeUndoRecord(rec *UndoRecord) []byte {
	if rec == nil {
		return nil
	}
	buf := make([]byte, UndoRecordHeaderSize+len(rec.Data))
	buf[undoHeaderTypeOffset] = rec.Type
	buf[undoHeaderCmplInfoOffset] = rec.CmplInfo
	if rec.UpdatedExtern {
		buf[undoHeaderExternOffset] = 1
	}
	binary.BigEndian.PutUint64(buf[undoHeaderUndoNoOffset:undoHeaderTableIDOffset], rec.UndoNo)
	binary.BigEndian.PutUint64(buf[undoHeaderTableIDOffset:UndoRecordHeaderSize], rec.TableID)
	copy(buf[UndoRecordHeaderSize:], rec.Data)
	return buf
}

// DecodeUndoRecord parses an undo record from a byte slice.
func DecodeUndoRecord(buf []byte) (*UndoRecord, error) {
	if len(buf) < undoRecordMinEncodedBytes {
		return nil, ErrUndoRecordTooShort
	}
	rec := &UndoRecord{
		Type:          buf[undoHeaderTypeOffset],
		CmplInfo:      buf[undoHeaderCmplInfoOffset],
		UpdatedExtern: buf[undoHeaderExternOffset] != 0,
		UndoNo:        binary.BigEndian.Uint64(buf[undoHeaderUndoNoOffset:undoHeaderTableIDOffset]),
		TableID:       binary.BigEndian.Uint64(buf[undoHeaderTableIDOffset:UndoRecordHeaderSize]),
	}
	if len(buf) > UndoRecordHeaderSize {
		rec.Data = append([]byte(nil), buf[UndoRecordHeaderSize:]...)
	}
	return rec, nil
}

// UndoRecordType reads the record type from a buffer.
func UndoRecordType(buf []byte) (uint8, error) {
	if len(buf) < 1 {
		return 0, ErrUndoRecordTooShort
	}
	return buf[undoHeaderTypeOffset], nil
}

// UndoRecordCmplInfo reads the compiler info from a buffer.
func UndoRecordCmplInfo(buf []byte) (uint8, error) {
	if len(buf) < 2 {
		return 0, ErrUndoRecordTooShort
	}
	return buf[undoHeaderCmplInfoOffset], nil
}

// UndoRecordUpdatedExtern reports whether extern storage was updated.
func UndoRecordUpdatedExtern(buf []byte) (bool, error) {
	if len(buf) < 3 {
		return false, ErrUndoRecordTooShort
	}
	return buf[undoHeaderExternOffset] != 0, nil
}

// UndoRecordUndoNo reads the undo number from a buffer.
func UndoRecordUndoNo(buf []byte) (uint64, error) {
	if len(buf) < undoHeaderTableIDOffset {
		return 0, ErrUndoRecordTooShort
	}
	return binary.BigEndian.Uint64(buf[undoHeaderUndoNoOffset:undoHeaderTableIDOffset]), nil
}

// UndoRecordTableID reads the table id from a buffer.
func UndoRecordTableID(buf []byte) (uint64, error) {
	if len(buf) < UndoRecordHeaderSize {
		return 0, ErrUndoRecordTooShort
	}
	return binary.BigEndian.Uint64(buf[undoHeaderTableIDOffset:UndoRecordHeaderSize]), nil
}
