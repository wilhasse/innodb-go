package log

import (
	"encoding/binary"
	"errors"
)

const recordHeaderSize = 1 + 4 + 4 + 4

// Record is a minimal redo log record.
type Record struct {
	Type    byte
	SpaceID uint32
	PageNo  uint32
	Payload []byte
}

var errShortRecord = errors.New("log: short record")

// EncodeRecord serializes a redo log record.
func EncodeRecord(rec Record) []byte {
	payloadLen := len(rec.Payload)
	buf := make([]byte, recordHeaderSize+payloadLen)
	buf[0] = rec.Type
	binary.BigEndian.PutUint32(buf[1:], rec.SpaceID)
	binary.BigEndian.PutUint32(buf[5:], rec.PageNo)
	binary.BigEndian.PutUint32(buf[9:], uint32(payloadLen))
	if payloadLen > 0 {
		copy(buf[recordHeaderSize:], rec.Payload)
	}
	return buf
}

// DecodeRecord parses a redo log record from buf.
func DecodeRecord(buf []byte) (Record, int, error) {
	if len(buf) < recordHeaderSize {
		return Record{}, 0, errShortRecord
	}
	rec := Record{
		Type:    buf[0],
		SpaceID: binary.BigEndian.Uint32(buf[1:]),
		PageNo:  binary.BigEndian.Uint32(buf[5:]),
	}
	payloadLen := binary.BigEndian.Uint32(buf[9:])
	total := recordHeaderSize + int(payloadLen)
	if len(buf) < total {
		return Record{}, 0, errShortRecord
	}
	if payloadLen > 0 {
		rec.Payload = append([]byte(nil), buf[recordHeaderSize:total]...)
	}
	return rec, total, nil
}
