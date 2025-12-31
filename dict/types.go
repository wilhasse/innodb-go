package dict

import (
	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/ut"
)

// Column describes a dictionary column.
type Column struct {
	Name string
	Type data.DataType
}

// Index describes a dictionary index.
type Index struct {
	Name      string
	ID        ut.Dulint
	Fields    []string
	Unique    bool
	Clustered bool
	RootPage  uint32
	TreeLevel uint32
	SpaceID   uint32
	ZipSize   uint32
}

// Table describes a dictionary table.
type Table struct {
	Name    string
	ID      ut.Dulint
	Space   uint32
	Flags   uint32
	Columns []Column
	NDef    int
	Indexes map[string]*Index
}
