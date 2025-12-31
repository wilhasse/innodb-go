package dict

import "testing"

func TestDictGetFirstTableNameInDB(t *testing.T) {
	DictInitCore()
	_ = DictTableAddToCache(&Table{Name: "db/b"})
	_ = DictTableAddToCache(&Table{Name: "db/a"})
	_ = DictTableAddToCache(&Table{Name: "other/x"})

	if got := DictGetFirstTableNameInDB("db/"); got != "db/a" {
		t.Fatalf("expected db/a, got %s", got)
	}
}

func TestDictListTables(t *testing.T) {
	DictInitCore()
	_ = DictTableAddToCache(&Table{Name: "db/b"})
	_ = DictTableAddToCache(&Table{Name: "db/a"})

	names := DictListTables()
	if len(names) != 2 || names[0] != "db/a" || names[1] != "db/b" {
		t.Fatalf("unexpected table list: %v", names)
	}
}

func TestDictLoadTable(t *testing.T) {
	DictInitCore()
	_ = DictTableAddToCache(&Table{Name: "db/a"})

	if _, err := DictLoadTable("db/a"); err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if _, err := DictLoadTable("missing"); err != ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound, got %v", err)
	}
}

func TestDictLoadSysTable(t *testing.T) {
	DictInitCore()
	table := &Table{Name: "SYS_TEST"}
	if err := DictLoadSysTable(table); err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if got := DictTableGet("SYS_TEST"); got == nil {
		t.Fatalf("expected sys table to be cached")
	}
}
