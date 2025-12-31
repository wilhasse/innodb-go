package dict

import "github.com/wilhasse/innodb-go/data"

// MemTableCreate creates a table metadata object.
func MemTableCreate(name string, space uint32, nCols int, flags uint32) *Table {
	if nCols < 0 {
		nCols = 0
	}
	return &Table{
		Name:    name,
		Space:   space,
		Flags:   flags,
		Columns: make([]Column, 0, nCols),
		Indexes: make(map[string]*Index),
	}
}

// MemTableFree releases resources for a table (no-op in Go).
func MemTableFree(table *Table) {
	_ = table
}

// MemTableAddCol adds a column definition to a table.
func MemTableAddCol(table *Table, name string, mtype, prtype, length uint32) {
	if table == nil {
		return
	}
	table.Columns = append(table.Columns, Column{
		Name: name,
		Type: data.DataType{
			MType:  mtype,
			PrType: prtype,
			Len:    length,
		},
	})
	table.NDef++
}

// MemIndexCreate creates an index metadata object.
func MemIndexCreate(tableName, indexName string, space uint32, flags uint32, nFields int) *Index {
	if nFields < 0 {
		nFields = 0
	}
	return &Index{
		Name:      indexName,
		Fields:    make([]string, 0, nFields),
		Unique:    flags&DictIndexUnique != 0,
		Clustered: flags&DictIndexClustered != 0,
	}
}

// MemIndexAddField adds a field to an index definition.
func MemIndexAddField(index *Index, name string) {
	if index == nil {
		return
	}
	index.Fields = append(index.Fields, name)
}
