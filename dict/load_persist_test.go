package dict

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
)

func TestDictLoadPersistedTables(t *testing.T) {
	dir := t.TempDir()
	SetDataDir(dir)
	DictBootstrap()

	table := &Table{
		Name:  "db/t2",
		ID:    DulintFromUint64(77),
		Space: 9,
		Columns: []Column{
			{Name: "c1", Type: data.DataType{MType: data.DataInt, PrType: 0, Len: 4}},
		},
		Indexes: make(map[string]*Index),
	}
	index := &Index{
		Name:      "PRIMARY",
		ID:        DulintFromUint64(88),
		Fields:    []string{"c1"},
		Unique:    true,
		Clustered: true,
		RootPage:  5,
		SpaceID:   9,
	}
	table.Indexes[index.Name] = index

	if err := DictPersistTableCreate(table); err != nil {
		t.Fatalf("persist create: %v", err)
	}
	DictClose()

	DictBootstrap()
	loaded := DictTableGet(table.Name)
	if loaded == nil {
		t.Fatalf("expected table after reload")
	}
	if len(loaded.Columns) != 1 || loaded.Columns[0].Name != "c1" {
		t.Fatalf("unexpected columns after reload")
	}
	loadedIdx := loaded.Indexes[index.Name]
	if loadedIdx == nil || len(loadedIdx.Fields) != 1 || loadedIdx.Fields[0] != "c1" {
		t.Fatalf("unexpected index fields after reload")
	}
}
