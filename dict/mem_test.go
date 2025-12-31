package dict

import "testing"

func TestMemTableCreateAddCol(t *testing.T) {
	table := MemTableCreate("t1", 1, 2, 0)
	if table == nil || table.Name != "t1" {
		t.Fatalf("expected table to be created")
	}
	MemTableAddCol(table, "c1", 1, 0, 4)
	MemTableAddCol(table, "c2", 2, 0, 8)
	if len(table.Columns) != 2 || table.NDef != 2 {
		t.Fatalf("expected two columns, got %d", len(table.Columns))
	}
	if table.Columns[0].Name != "c1" || table.Columns[1].Name != "c2" {
		t.Fatalf("unexpected column names")
	}
}

func TestMemIndexCreateAddField(t *testing.T) {
	index := MemIndexCreate("t1", "idx", 1, DictIndexUnique|DictIndexClustered, 2)
	if index == nil || !index.Unique || !index.Clustered {
		t.Fatalf("expected index flags to be set")
	}
	MemIndexAddField(index, "c1")
	MemIndexAddField(index, "c2")
	if len(index.Fields) != 2 {
		t.Fatalf("expected two index fields")
	}
}
