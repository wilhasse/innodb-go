package rem

import (
	"bytes"

	"github.com/wilhasse/innodb-go/data"
)

// ColsAreEqual reports whether two column types can be compared.
func ColsAreEqual(col1, col2 data.DataType, checkCharset bool) bool {
	if data.DataTypeIsNonBinaryStringType(col1.MType, col1.PrType) &&
		data.DataTypeIsNonBinaryStringType(col2.MType, col2.PrType) {
		if checkCharset {
			return charsetColl(col1.PrType) == charsetColl(col2.PrType)
		}
		return true
	}
	if data.DataTypeIsBinaryStringType(col1.MType, col1.PrType) &&
		data.DataTypeIsBinaryStringType(col2.MType, col2.PrType) {
		return true
	}
	if col1.MType != col2.MType {
		return false
	}
	if col1.MType == data.DataInt &&
		(col1.PrType&data.DataUnsigned) != (col2.PrType&data.DataUnsigned) {
		return false
	}
	return col1.MType != data.DataInt || col1.Len == col2.Len
}

// CompareData compares two data fields with explicit type info.
func CompareData(mtype, prtype uint32, data1 []byte, len1 uint32, data2 []byte, len2 uint32) int {
	if len1 == data.UnivSQLNull && len2 == data.UnivSQLNull {
		return 0
	}
	if len1 == data.UnivSQLNull {
		return 1
	}
	if len2 == data.UnivSQLNull {
		return -1
	}
	a := sliceByLen(data1, len1)
	b := sliceByLen(data2, len2)

	switch mtype {
	case data.DataInt:
		return compareInt(a, b, (prtype&data.DataUnsigned) != 0)
	case data.DataChar:
		return compareChar(a, b)
	case data.DataVarchar, data.DataBinary, data.DataFixBinary, data.DataBlob:
		return compareBytes(a, b)
	default:
		return compareBytes(a, b)
	}
}

// CompareFields compares two data fields using the type of the first field.
func CompareFields(field1, field2 *data.Field) int {
	if field1 == nil || field2 == nil {
		return 0
	}
	return CompareData(field1.Type.MType, field1.Type.PrType,
		field1.Data, field1.Len, field2.Data, field2.Len)
}

// CompareTuples compares tuples field by field.
func CompareTuples(t1, t2 *data.Tuple) int {
	if t1 == nil || t2 == nil {
		return 0
	}
	n := len(t1.Fields)
	if len(t2.Fields) < n {
		n = len(t2.Fields)
	}
	for i := 0; i < n; i++ {
		cmp := CompareFields(&t1.Fields[i], &t2.Fields[i])
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

func compareInt(a, b []byte, unsigned bool) int {
	if unsigned {
		u1 := decodeUint(a)
		u2 := decodeUint(b)
		switch {
		case u1 > u2:
			return 1
		case u1 < u2:
			return -1
		default:
			return 0
		}
	}
	i1 := decodeInt(a)
	i2 := decodeInt(b)
	switch {
	case i1 > i2:
		return 1
	case i1 < i2:
		return -1
	default:
		return 0
	}
}

func compareChar(a, b []byte) int {
	a = bytes.TrimRight(a, " ")
	b = bytes.TrimRight(b, " ")
	return compareBytes(a, b)
}

func compareBytes(a, b []byte) int {
	switch cmp := bytes.Compare(a, b); {
	case cmp > 0:
		return 1
	case cmp < 0:
		return -1
	default:
		return 0
	}
}

func sliceByLen(dataBytes []byte, length uint32) []byte {
	if length == data.UnivSQLNull {
		return nil
	}
	if dataBytes == nil {
		return nil
	}
	if int(length) <= len(dataBytes) {
		return dataBytes[:length]
	}
	return dataBytes
}

func decodeUint(dataBytes []byte) uint64 {
	var out uint64
	for _, b := range dataBytes {
		out = (out << 8) | uint64(b)
	}
	return out
}

func decodeInt(dataBytes []byte) int64 {
	if len(dataBytes) == 0 {
		return 0
	}
	u := decodeUint(dataBytes)
	bits := uint(len(dataBytes) * 8)
	if bits < 64 {
		signBit := uint64(1) << (bits - 1)
		if u&signBit != 0 {
			u |= ^uint64(0) << bits
		}
	}
	return int64(u)
}

func charsetColl(prtype uint32) uint32 {
	return prtype >> 16
}
