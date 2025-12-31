package ddl

import "testing"

func TestDDLCreateDropTable(t *testing.T) {
	mgr := NewManager()
	if _, err := mgr.CreateTable("t1", nil); err != nil {
		t.Fatalf("unexpected create error: %v", err)
	}
	if _, err := mgr.CreateTable("t1", nil); err != ErrTableExists {
		t.Fatalf("expected ErrTableExists, got %v", err)
	}
	if err := mgr.DropTable("t1"); err != nil {
		t.Fatalf("unexpected drop error: %v", err)
	}
	if err := mgr.DropTable("t1"); err != ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound, got %v", err)
	}
}

func TestDDLIndexOperations(t *testing.T) {
	mgr := NewManager()
	if _, err := mgr.CreateTable("t1", nil); err != nil {
		t.Fatalf("unexpected create error: %v", err)
	}
	index := Index{Name: "idx", Columns: []string{"c1"}, Unique: true}
	if err := mgr.CreateIndex("t1", index); err != nil {
		t.Fatalf("unexpected create index error: %v", err)
	}
	if err := mgr.CreateIndex("t1", index); err != ErrIndexExists {
		t.Fatalf("expected ErrIndexExists, got %v", err)
	}
	if err := mgr.DropIndex("t1", "idx"); err != nil {
		t.Fatalf("unexpected drop index error: %v", err)
	}
	if err := mgr.DropIndex("t1", "idx"); err != ErrIndexNotFound {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

func TestDDLRenameTable(t *testing.T) {
	mgr := NewManager()
	if _, err := mgr.CreateTable("t1", nil); err != nil {
		t.Fatalf("unexpected create error: %v", err)
	}
	if err := mgr.CreateIndex("t1", Index{Name: "idx"}); err != nil {
		t.Fatalf("unexpected create index error: %v", err)
	}
	if err := mgr.RenameTable("t1", "t2"); err != nil {
		t.Fatalf("unexpected rename error: %v", err)
	}
	if _, ok := mgr.GetTable("t1"); ok {
		t.Fatalf("expected old table name to be gone")
	}
	table, ok := mgr.GetTable("t2")
	if !ok {
		t.Fatalf("expected renamed table to exist")
	}
	if table.Indexes["idx"].Table != "t2" {
		t.Fatalf("expected index table name to be updated")
	}
}
