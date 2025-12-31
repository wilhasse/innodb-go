package rec

import (
	"encoding/binary"
	"errors"

	"github.com/wilhasse/innodb-go/data"
)

// DecodeFixed decodes fixed-length record bytes into a tuple.
func DecodeFixed(rec []byte, lengths []int, extra int) (*data.Tuple, error) {
	if extra < 0 {
		extra = 0
	}
	if len(rec) < extra {
		return nil, errors.New("rec: record too short")
	}
	pos := extra
	fields := make([]data.Field, len(lengths))
	for i, length := range lengths {
		if length < 0 {
			length = 0
		}
		if pos+length > len(rec) {
			return nil, errors.New("rec: truncated field")
		}
		dataBytes := make([]byte, length)
		copy(dataBytes, rec[pos:pos+length])
		fields[i] = data.Field{Data: dataBytes, Len: uint32(length)}
		pos += length
	}
	return &data.Tuple{
		NFields:    len(fields),
		NFieldsCmp: len(fields),
		Fields:     fields,
		Magic:      data.DataTupleMagic,
	}, nil
}

// DecodeVar decodes variable-length record bytes into a tuple.
func DecodeVar(rec []byte, nFields int, extra int) (*data.Tuple, error) {
	if nFields < 0 {
		return nil, errors.New("rec: invalid field count")
	}
	if extra < 0 {
		extra = 0
	}
	if len(rec) < extra {
		return nil, errors.New("rec: record too short")
	}
	pos := extra
	fields := make([]data.Field, nFields)
	for i := 0; i < nFields; i++ {
		if pos+3 > len(rec) {
			return nil, errors.New("rec: truncated header")
		}
		nullFlag := rec[pos]
		pos++
		length := int(binary.BigEndian.Uint16(rec[pos : pos+2]))
		pos += 2
		if nullFlag == 1 {
			fields[i].Len = data.UnivSQLNull
			continue
		}
		if nullFlag != 0 {
			return nil, errors.New("rec: invalid null flag")
		}
		if pos+length > len(rec) {
			return nil, errors.New("rec: truncated data")
		}
		dataBytes := make([]byte, length)
		copy(dataBytes, rec[pos:pos+length])
		pos += length
		fields[i] = data.Field{Data: dataBytes, Len: uint32(length)}
	}
	return &data.Tuple{
		NFields:    len(fields),
		NFieldsCmp: len(fields),
		Fields:     fields,
		Magic:      data.DataTupleMagic,
	}, nil
}
