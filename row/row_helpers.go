package row

import (
	"errors"

	"github.com/wilhasse/innodb-go/data"
)

// CopyMode controls how row data is copied.
type CopyMode int

const (
	CopyPointers CopyMode = iota
	CopyData
)

// BuildIndexEntry builds a tuple containing the indexed fields.
func BuildIndexEntry(row *data.Tuple, indexFields []int, ext *ExtCache) (*data.Tuple, error) {
	if row == nil {
		return nil, errors.New("row: nil row")
	}
	entry := &data.Tuple{
		Fields:     make([]data.Field, len(indexFields)),
		NFields:    len(indexFields),
		NFieldsCmp: len(indexFields),
		Magic:      data.DataTupleMagic,
	}
	for i, col := range indexFields {
		if col < 0 || col >= len(row.Fields) {
			return nil, errors.New("row: column out of range")
		}
		field := row.Fields[col]
		if field.Len != data.UnivSQLNull && field.Ext && ext != nil {
			if prefix := ext.Prefix(col); prefix != nil {
				field.Data = prefix
				field.Len = uint32(len(prefix))
			}
		}
		entry.Fields[i] = field
	}
	return entry, nil
}

// CopyRow copies a row tuple according to the requested mode.
func CopyRow(row *data.Tuple, mode CopyMode) *data.Tuple {
	if row == nil {
		return nil
	}
	copyTuple := &data.Tuple{
		InfoBits:   row.InfoBits,
		NFields:    len(row.Fields),
		NFieldsCmp: row.NFieldsCmp,
		Magic:      row.Magic,
		Fields:     make([]data.Field, len(row.Fields)),
	}
	for i, field := range row.Fields {
		copyTuple.Fields[i] = field
		if mode == CopyData && field.Data != nil && field.Len != data.UnivSQLNull {
			length := int(field.Len)
			if length > len(field.Data) {
				length = len(field.Data)
			}
			dup := make([]byte, length)
			copy(dup, field.Data[:length])
			copyTuple.Fields[i].Data = dup
		}
	}
	return copyTuple
}
