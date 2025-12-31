package dict

import (
	"path/filepath"
	"testing"

	"github.com/wilhasse/innodb-go/data"
	ibos "github.com/wilhasse/innodb-go/os"
)

func TestDictPersistCreateDrop(t *testing.T) {
	dir := t.TempDir()
	SetDataDir(dir)
	DictBootstrap()

	table := &Table{
		Name:  "db/t1",
		ID:    DulintFromUint64(42),
		Space: 7,
		Columns: []Column{
			{Name: "c1", Type: data.DataType{MType: data.DataInt, PrType: 0, Len: 4}},
		},
		Indexes: make(map[string]*Index),
	}
	index := &Index{
		Name:      "PRIMARY",
		ID:        DulintFromUint64(99),
		Fields:    []string{"c1"},
		Unique:    true,
		Clustered: true,
		RootPage:  3,
		SpaceID:   7,
	}
	table.Indexes[index.Name] = index

	if err := DictPersistTableCreate(table); err != nil {
		t.Fatalf("persist create: %v", err)
	}
	if DictTableGet(table.Name) == nil {
		t.Fatalf("expected table in cache")
	}
	if !sysRowHasTable(table.Name) {
		t.Fatalf("expected SYS_TABLES row for table")
	}
	if exists, _ := ibos.FileExists(filepath.Join(dir, dictFileName)); !exists {
		t.Fatalf("expected dict file to exist")
	}

	if err := DictPersistTableDrop(table); err != nil {
		t.Fatalf("persist drop: %v", err)
	}
	if DictTableGet(table.Name) != nil {
		t.Fatalf("expected table removed from cache")
	}
	if sysRowHasTable(table.Name) {
		t.Fatalf("expected SYS_TABLES row removed")
	}
}

func sysRowHasTable(name string) bool {
	if DictSys == nil {
		return false
	}
	for _, row := range DictSys.SysRows.Tables {
		if row == nil || len(row.Fields) == 0 {
			continue
		}
		if string(row.Fields[0].Data) == name {
			return true
		}
	}
	return false
}
