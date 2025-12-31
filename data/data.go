package data

import (
	"bytes"
)

// Data type constants.
const (
	DataVarchar   = 1
	DataChar      = 2
	DataFixBinary = 3
	DataBinary    = 4
	DataBlob      = 5
	DataInt       = 6
	DataSysChild  = 7
	DataSys       = 8
	DataFloat     = 9
	DataDouble    = 10
	DataDecimal   = 11
	DataVarClient = 12
	DataClient    = 13
	DataMTypeMax  = 63
	DataError     = 111
)

// DataTupleMagic mirrors DATA_TUPLE_MAGIC_N.
const DataTupleMagic = 65478679

// UnivSQLNull mirrors UNIV_SQL_NULL.
const UnivSQLNull = uint32(0xFFFFFFFF)

// UnivExternStorageField mirrors UNIV_EXTERN_STORAGE_FIELD.
const UnivExternStorageField = UnivSQLNull - 16384

// RecMaxNFields mirrors REC_MAX_N_FIELDS.
const RecMaxNFields = 1023

// DataType mirrors dtype_t.
type DataType struct {
	MType    uint32
	PrType   uint32
	Len      uint32
	MbMinLen uint32
	MbMaxLen uint32
}

// Field mirrors dfield_t.
type Field struct {
	Data []byte
	Ext  bool
	Len  uint32
	Type DataType
}

// Tuple mirrors dtuple_t.
type Tuple struct {
	InfoBits   uint32
	NFields    int
	NFieldsCmp int
	Fields     []Field
	Magic      uint32
}

// BigRecField mirrors big_rec_field_t.
type BigRecField struct {
	FieldNo uint32
	Len     uint32
	Data    []byte
}

// BigRec mirrors big_rec_t.
type BigRec struct {
	NFields uint32
	Fields  []BigRecField
}

// FieldGetType returns the field type.
func FieldGetType(field *Field) *DataType {
	if field == nil {
		return nil
	}
	return &field.Type
}

// FieldSetType sets the field type.
func FieldSetType(field *Field, typ DataType) {
	if field == nil {
		return
	}
	field.Type = typ
}

// FieldGetData returns the field data.
func FieldGetData(field *Field) []byte {
	if field == nil {
		return nil
	}
	return field.Data
}

// FieldGetLen returns the data length.
func FieldGetLen(field *Field) uint32 {
	if field == nil {
		return UnivSQLNull
	}
	return field.Len
}

// FieldSetLen sets the data length.
func FieldSetLen(field *Field, length uint32) {
	if field == nil {
		return
	}
	field.Len = length
}

// FieldIsNull reports whether the field is SQL NULL.
func FieldIsNull(field *Field) bool {
	if field == nil {
		return true
	}
	return field.Len == UnivSQLNull
}

// FieldIsExt reports whether the field is externally stored.
func FieldIsExt(field *Field) bool {
	if field == nil {
		return false
	}
	return field.Ext
}

// FieldSetExt marks the field as externally stored.
func FieldSetExt(field *Field) {
	if field == nil {
		return
	}
	field.Ext = true
}

// FieldSetData sets the data pointer and length.
func FieldSetData(field *Field, data []byte, length uint32) {
	if field == nil {
		return
	}
	if length == UnivSQLNull {
		FieldSetNull(field)
		return
	}
	if data != nil && int(length) <= len(data) {
		field.Data = data[:length]
	} else {
		field.Data = data
	}
	field.Len = length
}

// FieldSetNull sets the field to SQL NULL.
func FieldSetNull(field *Field) {
	if field == nil {
		return
	}
	field.Data = nil
	field.Len = UnivSQLNull
}

// WriteSQLNull writes a SQL NULL field full of zeros.
func WriteSQLNull(data []byte) {
	for i := range data {
		data[i] = 0
	}
}

// FieldCopyData copies data pointer and length.
func FieldCopyData(dst, src *Field) {
	if dst == nil || src == nil {
		return
	}
	dst.Data = src.Data
	dst.Len = src.Len
}

// FieldCopy copies a field including type and flags.
func FieldCopy(dst, src *Field) {
	if dst == nil || src == nil {
		return
	}
	*dst = *src
}

// FieldDup copies field data into a new slice.
func FieldDup(field *Field) {
	if field == nil || field.Data == nil || FieldIsNull(field) {
		return
	}
	dup := make([]byte, len(field.Data))
	copy(dup, field.Data)
	field.Data = dup
}

// FieldDatasAreBinaryEqual compares two fields.
func FieldDatasAreBinaryEqual(a, b *Field) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Len != b.Len {
		return false
	}
	if a.Len == UnivSQLNull {
		return true
	}
	return bytes.Equal(a.Data, b.Data)
}

// FieldDataIsBinaryEqual compares field data to the provided bytes.
func FieldDataIsBinaryEqual(field *Field, length uint32, data []byte) bool {
	if field == nil {
		return false
	}
	if field.Len != length {
		return false
	}
	if length == UnivSQLNull {
		return true
	}
	return bytes.Equal(field.Data, data)
}

// NewTuple allocates a data tuple with the given field count.
func NewTuple(nFields int) *Tuple {
	if nFields < 0 {
		nFields = 0
	}
	return &Tuple{
		NFields:    nFields,
		NFieldsCmp: nFields,
		Fields:     make([]Field, nFields),
		Magic:      DataTupleMagic,
	}
}

// TupleGetNFields returns the number of fields.
func TupleGetNFields(tuple *Tuple) int {
	if tuple == nil {
		return 0
	}
	return tuple.NFields
}

// TupleGetNthField returns the nth field.
func TupleGetNthField(tuple *Tuple, n int) *Field {
	if tuple == nil || n < 0 || n >= tuple.NFields {
		return nil
	}
	return &tuple.Fields[n]
}

// TupleSetNFields sets the number of fields and comparison fields.
func TupleSetNFields(tuple *Tuple, nFields int) {
	if tuple == nil {
		return
	}
	if nFields < 0 {
		nFields = 0
	}
	if nFields > len(tuple.Fields) {
		fields := make([]Field, nFields)
		copy(fields, tuple.Fields)
		tuple.Fields = fields
	}
	tuple.NFields = nFields
	tuple.NFieldsCmp = nFields
}

// TupleCheckTyped validates field types in the tuple.
func TupleCheckTyped(tuple *Tuple) bool {
	if tuple == nil {
		return false
	}
	if tuple.NFields > RecMaxNFields {
		return false
	}
	for i := 0; i < tuple.NFields; i++ {
		field := &tuple.Fields[i]
		if field.Type.MType < DataVarchar || field.Type.MType > DataClient {
			return false
		}
	}
	return true
}

// CompareFields compares two fields lexicographically.
func CompareFields(a, b *Field) int {
	if a == nil || b == nil {
		switch {
		case a == b:
			return 0
		case a == nil:
			return -1
		default:
			return 1
		}
	}
	if FieldIsNull(a) || FieldIsNull(b) {
		switch {
		case FieldIsNull(a) && FieldIsNull(b):
			return 0
		case FieldIsNull(a):
			return -1
		default:
			return 1
		}
	}
	cmp := bytes.Compare(a.Data, b.Data)
	if cmp != 0 {
		return cmp
	}
	if a.Len == b.Len {
		return 0
	}
	if a.Len < b.Len {
		return -1
	}
	return 1
}

// TupleCollCmp compares two tuples field by field.
func TupleCollCmp(tuple1, tuple2 *Tuple) int {
	if tuple1 == nil || tuple2 == nil {
		switch {
		case tuple1 == tuple2:
			return 0
		case tuple1 == nil:
			return -1
		default:
			return 1
		}
	}
	if tuple1.NFields != tuple2.NFields {
		if tuple1.NFields < tuple2.NFields {
			return -1
		}
		return 1
	}
	for i := 0; i < tuple1.NFields; i++ {
		cmp := CompareFields(&tuple1.Fields[i], &tuple2.Fields[i])
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}
