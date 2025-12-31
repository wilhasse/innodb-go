package rec

import (
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
