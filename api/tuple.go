package api

import (
	"encoding/binary"
	"math"

	"github.com/wilhasse/innodb-go/data"
)

// TupleWriteU32 writes a uint32 value into a tuple.
func TupleWriteU32(tpl *data.Tuple, col int, val uint32) ErrCode {
	if tpl == nil || col < 0 || col >= len(tpl.Fields) {
		return DB_ERROR
	}
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], val)
	tpl.Fields[col].Data = append([]byte(nil), buf[:]...)
	tpl.Fields[col].Len = 4
	return DB_SUCCESS
}

// TupleReadU32 reads a uint32 value from a tuple.
func TupleReadU32(tpl *data.Tuple, col int, out *uint32) ErrCode {
	if tpl == nil || out == nil || col < 0 || col >= len(tpl.Fields) {
		return DB_ERROR
	}
	field := tpl.Fields[col]
	if len(field.Data) < 4 {
		return DB_ERROR
	}
	*out = binary.BigEndian.Uint32(field.Data)
	return DB_SUCCESS
}

// TupleWriteU64 writes a uint64 value into a tuple.
func TupleWriteU64(tpl *data.Tuple, col int, val uint64) ErrCode {
	if tpl == nil || col < 0 || col >= len(tpl.Fields) {
		return DB_ERROR
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], val)
	tpl.Fields[col].Data = append([]byte(nil), buf[:]...)
	tpl.Fields[col].Len = 8
	return DB_SUCCESS
}

// TupleReadU64 reads a uint64 value from a tuple.
func TupleReadU64(tpl *data.Tuple, col int, out *uint64) ErrCode {
	if tpl == nil || out == nil || col < 0 || col >= len(tpl.Fields) {
		return DB_ERROR
	}
	field := tpl.Fields[col]
	if len(field.Data) < 8 {
		return DB_ERROR
	}
	*out = binary.BigEndian.Uint64(field.Data)
	return DB_SUCCESS
}

// TupleWriteDouble writes a float64 value into a tuple.
func TupleWriteDouble(tpl *data.Tuple, col int, val float64) ErrCode {
	if tpl == nil || col < 0 || col >= len(tpl.Fields) {
		return DB_ERROR
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], math.Float64bits(val))
	tpl.Fields[col].Data = append([]byte(nil), buf[:]...)
	tpl.Fields[col].Len = 8
	return DB_SUCCESS
}

// TupleReadDouble reads a float64 value from a tuple.
func TupleReadDouble(tpl *data.Tuple, col int, out *float64) ErrCode {
	if tpl == nil || out == nil || col < 0 || col >= len(tpl.Fields) {
		return DB_ERROR
	}
	field := tpl.Fields[col]
	if len(field.Data) < 8 {
		return DB_ERROR
	}
	*out = math.Float64frombits(binary.BigEndian.Uint64(field.Data))
	return DB_SUCCESS
}

// ColSetValue sets raw column data for a tuple.
func ColSetValue(tpl *data.Tuple, col int, value []byte, length int) ErrCode {
	if tpl == nil || col < 0 || col >= len(tpl.Fields) || length < 0 {
		return DB_ERROR
	}
	if length == int(IBSQLNull) {
		tpl.Fields[col].Data = nil
		tpl.Fields[col].Len = data.UnivSQLNull
		return DB_SUCCESS
	}
	if length == 0 {
		tpl.Fields[col].Data = nil
		tpl.Fields[col].Len = 0
		return DB_SUCCESS
	}
	if value == nil || length > len(value) {
		return DB_ERROR
	}
	tpl.Fields[col].Data = append([]byte(nil), value[:length]...)
	tpl.Fields[col].Len = uint32(length)
	return DB_SUCCESS
}

// ColGetValue returns the raw column bytes.
func ColGetValue(tpl *data.Tuple, col int) []byte {
	if tpl == nil || col < 0 || col >= len(tpl.Fields) {
		return nil
	}
	field := tpl.Fields[col]
	if field.Len == data.UnivSQLNull {
		return nil
	}
	if int(field.Len) <= len(field.Data) {
		return field.Data[:field.Len]
	}
	return field.Data
}

// ColGetLen returns the column length or IBSQLNull for NULLs.
func ColGetLen(tpl *data.Tuple, col int) Ulint {
	if tpl == nil || col < 0 || col >= len(tpl.Fields) {
		return Ulint(IBSQLNull)
	}
	field := tpl.Fields[col]
	if field.Len == data.UnivSQLNull {
		return Ulint(IBSQLNull)
	}
	return Ulint(field.Len)
}
