package dict

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
)

func TestRebuildFromSysRowsSkipsInvalidMetadata(t *testing.T) {
	DictInit()
	dictHdrCreate()
	createSysTables()

	table := &Table{
		Name:  "db/t_bad",
		ID:    DulintFromUint64(10),
		Space: 7,
		Columns: []Column{
			{Name: "c1", Type: data.DataType{MType: data.DataInt, Len: 4}},
		},
		Indexes: make(map[string]*Index),
	}
	index := &Index{
		Name:      "PRIMARY",
		ID:        DulintFromUint64(20),
		Fields:    []string{"c1"},
		Unique:    true,
		Clustered: true,
		RootPage:  3,
		SpaceID:   7,
	}
	table.Indexes[index.Name] = index

	tablesRow := CreateSysTablesTuple(table)
	data.FieldSetData(&tablesRow.Fields[2], writeUint32(2), 4)

	DictSys.SysRows = SysRows{
		Tables:  []*data.Tuple{tablesRow},
		Columns: []*data.Tuple{CreateSysColumnsTuple(table, 0)},
		Indexes: []*data.Tuple{CreateSysIndexesTuple(table, index)},
		Fields:  []*data.Tuple{CreateSysFieldsTuple(index, 0)},
	}
	rebuildFromSysRows()

	if got := DictTableGet(table.Name); got != nil {
		t.Fatalf("expected invalid table skipped")
	}
}
