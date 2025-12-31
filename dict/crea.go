package dict

import (
	"encoding/binary"
	"errors"

	"github.com/wilhasse/innodb-go/data"
)

var (
	ErrInvalidName   = errors.New("dict: invalid name")
	ErrTableExists   = errors.New("dict: table already exists")
	ErrTableNotFound = errors.New("dict: table not found")
	ErrIndexExists   = errors.New("dict: index already exists")
)

// Index type flags.
const (
	DictIndexUnique    = 1 << 0
	DictIndexClustered = 1 << 1
)

// CreateSysTablesTuple builds the SYS_TABLES entry for the table.
func CreateSysTablesTuple(table *Table) *data.Tuple {
	if table == nil {
		return nil
	}
	entry := data.NewTuple(8)
	data.FieldSetData(&entry.Fields[0], []byte(table.Name), uint32(len(table.Name)))
	data.FieldSetData(&entry.Fields[1], writeUint64(dulintToUint64(table.ID)), 8)

	nCols := uint32(len(table.Columns))
	data.FieldSetData(&entry.Fields[2], writeUint32(nCols), 4)

	data.FieldSetData(&entry.Fields[3], writeUint32(table.Flags), 4)

	data.FieldSetData(&entry.Fields[4], make([]byte, 8), 8)
	data.FieldSetData(&entry.Fields[5], writeUint32(0), 4)

	data.FieldSetNull(&entry.Fields[6])
	data.FieldSetData(&entry.Fields[7], writeUint32(table.Space), 4)
	return entry
}

// CreateSysColumnsTuple builds the SYS_COLUMNS entry for the column.
func CreateSysColumnsTuple(table *Table, colIndex int) *data.Tuple {
	if table == nil || colIndex < 0 || colIndex >= len(table.Columns) {
		return nil
	}
	column := table.Columns[colIndex]
	entry := data.NewTuple(7)

	data.FieldSetData(&entry.Fields[0], writeUint64(dulintToUint64(table.ID)), 8)
	data.FieldSetData(&entry.Fields[1], writeUint32(uint32(colIndex)), 4)
	data.FieldSetData(&entry.Fields[2], []byte(column.Name), uint32(len(column.Name)))
	data.FieldSetData(&entry.Fields[3], writeUint32(column.Type.MType), 4)
	data.FieldSetData(&entry.Fields[4], writeUint32(column.Type.PrType), 4)
	data.FieldSetData(&entry.Fields[5], writeUint32(column.Type.Len), 4)
	data.FieldSetData(&entry.Fields[6], writeUint32(0), 4)
	return entry
}

// CreateSysIndexesTuple builds the SYS_INDEXES entry for the index.
func CreateSysIndexesTuple(table *Table, index *Index) *data.Tuple {
	if table == nil || index == nil {
		return nil
	}
	entry := data.NewTuple(7)
	data.FieldSetData(&entry.Fields[0], writeUint64(dulintToUint64(table.ID)), 8)
	data.FieldSetData(&entry.Fields[1], writeUint64(dulintToUint64(index.ID)), 8)
	data.FieldSetData(&entry.Fields[2], []byte(index.Name), uint32(len(index.Name)))
	data.FieldSetData(&entry.Fields[3], writeUint32(uint32(len(index.Fields))), 4)

	indexType := uint32(0)
	if index.Unique {
		indexType |= DictIndexUnique
	}
	if index.Clustered {
		indexType |= DictIndexClustered
	}
	data.FieldSetData(&entry.Fields[4], writeUint32(indexType), 4)
	data.FieldSetData(&entry.Fields[5], writeUint32(table.Space), 4)
	data.FieldSetData(&entry.Fields[6], writeUint32(index.RootPage), 4)
	return entry
}

// CreateSysFieldsTuple builds the SYS_FIELDS entry for an index field.
func CreateSysFieldsTuple(index *Index, fieldIndex int) *data.Tuple {
	if index == nil || fieldIndex < 0 || fieldIndex >= len(index.Fields) {
		return nil
	}
	entry := data.NewTuple(3)
	data.FieldSetData(&entry.Fields[0], writeUint64(dulintToUint64(index.ID)), 8)
	data.FieldSetData(&entry.Fields[1], writeUint32(uint32(fieldIndex)), 4)
	name := index.Fields[fieldIndex]
	data.FieldSetData(&entry.Fields[2], []byte(name), uint32(len(name)))
	return entry
}

// DictCreateTable registers a user table in the dictionary cache.
func DictCreateTable(table *Table) error {
	if table == nil || table.Name == "" {
		return ErrInvalidName
	}
	if DictSys == nil {
		DictInit()
	}
	DictSys.mu.Lock()
	defer DictSys.mu.Unlock()
	if _, exists := DictSys.Tables[table.Name]; exists {
		return ErrTableExists
	}
	DictSys.Tables[table.Name] = table
	return nil
}

// DictCreateIndex registers an index for a table.
func DictCreateIndex(tableName string, index *Index) error {
	if DictSys == nil {
		DictInit()
	}
	if index == nil || index.Name == "" {
		return ErrInvalidName
	}
	DictSys.mu.Lock()
	defer DictSys.mu.Unlock()
	table, ok := DictSys.Tables[tableName]
	if !ok {
		return ErrTableNotFound
	}
	if table.Indexes == nil {
		table.Indexes = make(map[string]*Index)
	}
	if _, exists := table.Indexes[index.Name]; exists {
		return ErrIndexExists
	}
	table.Indexes[index.Name] = index
	return nil
}

func writeUint32(value uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, value)
	return buf
}

func writeUint64(value uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, value)
	return buf
}
