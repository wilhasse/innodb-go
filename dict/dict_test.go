package dict

import "testing"

func TestDictLookupAndRename(t *testing.T) {
	DictInitCore()

	table := &Table{Name: "db/t1", ID: newDulint(0, 10), Indexes: make(map[string]*Index)}
	if err := DictTableAddToCache(table); err != nil {
		t.Fatalf("unexpected add error: %v", err)
	}
	if got := DictTableGet("db/t1"); got == nil {
		t.Fatalf("expected table lookup by name")
	}
	if got := DictTableGetOnID(newDulint(0, 10)); got == nil {
		t.Fatalf("expected table lookup by id")
	}
	if err := DictTableRenameInCache(table, "db/t2"); err != nil {
		t.Fatalf("unexpected rename error: %v", err)
	}
	if got := DictTableGet("db/t2"); got == nil {
		t.Fatalf("expected renamed table lookup")
	}
	DictTableRemoveFromCache(table)
	if got := DictTableGet("db/t2"); got != nil {
		t.Fatalf("expected table removal")
	}
}

func TestDictIndexCache(t *testing.T) {
	table := &Table{Name: "t1", Indexes: make(map[string]*Index)}
	index := &Index{Name: "idx"}
	if err := DictIndexAddToCache(table, index); err != nil {
		t.Fatalf("unexpected add index error: %v", err)
	}
	if err := DictIndexAddToCache(table, index); err != ErrIndexExists {
		t.Fatalf("expected ErrIndexExists, got %v", err)
	}
	DictIndexRemoveFromCache(table, index)
	if _, ok := table.Indexes["idx"]; ok {
		t.Fatalf("expected index removal")
	}
}

func TestDictNameHelpers(t *testing.T) {
	name := "db/table"
	if got := DictGetDBNameLen(name); got != 2 {
		t.Fatalf("expected db name length 2, got %d", got)
	}
	if got := DictRemoveDBName(name); got != "table" {
		t.Fatalf("unexpected remove db name result: %s", got)
	}
	if got := DictCasednStr("AbC"); got != "abc" {
		t.Fatalf("unexpected case down result: %s", got)
	}
}
