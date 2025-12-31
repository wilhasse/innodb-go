package rem

import (
	"encoding/binary"
	"fmt"

	"github.com/wilhasse/innodb-go/data"
)

// PackTuple encodes a tuple into a simple length-prefixed record format.
func PackTuple(tuple *data.Tuple) []byte {
	if tuple == nil {
		return nil
	}
	n := len(tuple.Fields)
	buf := make([]byte, 0, 2+n*5)
	var hdr [2]byte
	binary.BigEndian.PutUint16(hdr[:], uint16(n))
	buf = append(buf, hdr[:]...)

	for _, field := range tuple.Fields {
		if field.Len == data.UnivSQLNull {
			buf = append(buf, 1)
			continue
		}
		buf = append(buf, 0)
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], field.Len)
		buf = append(buf, lenBuf[:]...)
		if field.Data != nil && int(field.Len) <= len(field.Data) {
			buf = append(buf, field.Data[:field.Len]...)
		} else {
			buf = append(buf, field.Data...)
		}
	}
	return buf
}

// UnpackTuple decodes a tuple from a length-prefixed record buffer.
func UnpackTuple(buf []byte) (*data.Tuple, error) {
	if len(buf) < 2 {
		return nil, fmt.Errorf("rem: buffer too small")
	}
	n := int(binary.BigEndian.Uint16(buf[:2]))
	pos := 2
	fields := make([]data.Field, n)
	for i := 0; i < n; i++ {
		if pos >= len(buf) {
			return nil, fmt.Errorf("rem: truncated record")
		}
		flag := buf[pos]
		pos++
		if flag == 1 {
			fields[i].Len = data.UnivSQLNull
			continue
		}
		if pos+4 > len(buf) {
			return nil, fmt.Errorf("rem: truncated length")
		}
		length := binary.BigEndian.Uint32(buf[pos : pos+4])
		pos += 4
		if pos+int(length) > len(buf) {
			return nil, fmt.Errorf("rem: truncated data")
		}
		dataBytes := make([]byte, length)
		copy(dataBytes, buf[pos:pos+int(length)])
		pos += int(length)
		fields[i] = data.Field{Data: dataBytes, Len: length}
	}
	return &data.Tuple{
		NFields:    len(fields),
		NFieldsCmp: len(fields),
		Fields:     fields,
		Magic:      data.DataTupleMagic,
	}, nil
}
