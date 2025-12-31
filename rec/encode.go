package rec

import (
	"encoding/binary"
	"errors"

	"github.com/wilhasse/innodb-go/data"
)

// EncodeFixed encodes a tuple into record bytes for fixed-length columns.
// The output includes the requested extra bytes prefix.
func EncodeFixed(tuple *data.Tuple, lengths []int, extra int) ([]byte, error) {
	if tuple == nil {
		return nil, errors.New("rec: nil tuple")
	}
	if len(lengths) != len(tuple.Fields) {
		return nil, errors.New("rec: length mismatch")
	}
	if extra < 0 {
		extra = 0
	}
	dataLen := 0
	for _, length := range lengths {
		if length < 0 {
			length = 0
		}
		dataLen += length
	}
	buf := make([]byte, extra+dataLen)
	pos := extra
	for i, field := range tuple.Fields {
		length := lengths[i]
		if length < 0 {
			length = 0
		}
		if length == 0 {
			continue
		}
		if data.FieldIsNull(&field) {
			pos += length
			continue
		}
		copyLen := int(field.Len)
		if copyLen > len(field.Data) {
			copyLen = len(field.Data)
		}
		if copyLen > length {
			copyLen = length
		}
		copy(buf[pos:pos+length], field.Data[:copyLen])
		pos += length
	}
	return buf, nil
}

// EncodeVar encodes a tuple for variable-length fields with NULL flags.
// Each field is encoded as: nullFlag (1 byte), length (2 bytes), data.
func EncodeVar(tuple *data.Tuple, prefixes []int, extra int) ([]byte, error) {
	if tuple == nil {
		return nil, errors.New("rec: nil tuple")
	}
	if extra < 0 {
		extra = 0
	}
	buf := make([]byte, extra)
	for i, field := range tuple.Fields {
		prefix := 0
		if i < len(prefixes) {
			prefix = prefixes[i]
		}
		if data.FieldIsNull(&field) {
			buf = append(buf, 1)
			buf = appendUint16(buf, 0)
			continue
		}
		dataBytes := field.Data
		if int(field.Len) < len(dataBytes) {
			dataBytes = dataBytes[:field.Len]
		}
		if prefix > 0 && len(dataBytes) > prefix {
			dataBytes = dataBytes[:prefix]
		}
		buf = append(buf, 0)
		buf = appendUint16(buf, len(dataBytes))
		buf = append(buf, dataBytes...)
	}
	return buf, nil
}

func appendUint16(buf []byte, val int) []byte {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(val))
	return append(buf, tmp[:]...)
}
