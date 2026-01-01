package trx

import (
	"encoding/binary"
	"errors"
)

const undoPayloadHeaderSize = 16

// ErrUndoPayloadTooShort signals a truncated undo payload.
var ErrUndoPayloadTooShort = errors.New("trx: undo payload too short")

// UndoPayload stores the minimal undo record payload.
type UndoPayload struct {
	TrxID       uint64
	PrimaryKey  []byte
	BeforeImage []byte
}

// EncodeUndoPayload serializes an undo payload into a byte slice.
func EncodeUndoPayload(payload *UndoPayload) []byte {
	if payload == nil {
		return nil
	}
	pkLen := len(payload.PrimaryKey)
	beforeLen := len(payload.BeforeImage)
	buf := make([]byte, undoPayloadHeaderSize+pkLen+beforeLen)
	binary.BigEndian.PutUint64(buf[0:], payload.TrxID)
	binary.BigEndian.PutUint32(buf[8:], uint32(pkLen))
	binary.BigEndian.PutUint32(buf[12:], uint32(beforeLen))
	copy(buf[undoPayloadHeaderSize:], payload.PrimaryKey)
	copy(buf[undoPayloadHeaderSize+pkLen:], payload.BeforeImage)
	return buf
}

// DecodeUndoPayload parses an undo payload from a byte slice.
func DecodeUndoPayload(buf []byte) (*UndoPayload, error) {
	if len(buf) < undoPayloadHeaderSize {
		return nil, ErrUndoPayloadTooShort
	}
	trxID := binary.BigEndian.Uint64(buf[0:])
	pkLen := int(binary.BigEndian.Uint32(buf[8:]))
	beforeLen := int(binary.BigEndian.Uint32(buf[12:]))
	total := undoPayloadHeaderSize + pkLen + beforeLen
	if len(buf) < total {
		return nil, ErrUndoPayloadTooShort
	}
	payload := &UndoPayload{TrxID: trxID}
	if pkLen > 0 {
		payload.PrimaryKey = append([]byte(nil), buf[undoPayloadHeaderSize:undoPayloadHeaderSize+pkLen]...)
	}
	if beforeLen > 0 {
		payload.BeforeImage = append([]byte(nil), buf[undoPayloadHeaderSize+pkLen:total]...)
	}
	return payload, nil
}
