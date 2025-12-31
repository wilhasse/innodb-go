package row

import "testing"

func TestUndoManager(t *testing.T) {
	store := NewStore(-1)
	mgr := &UndoManager{}
	t1 := tupleKey(1)
	_ = store.Insert(t1)
	mgr.RecordInsert(t1)

	mgr.RecordModify(t1)
	t1.Fields[0].Data[0] = 2

	if err := mgr.UndoLast(store); err != nil {
		t.Fatalf("undo modify: %v", err)
	}
	if t1.Fields[0].Data[0] != 1 {
		t.Fatalf("expected restore")
	}

	if err := mgr.UndoLast(store); err != nil {
		t.Fatalf("undo insert: %v", err)
	}
	if len(store.Rows) != 0 {
		t.Fatalf("expected row removed")
	}

	if err := mgr.UndoLast(store); err != ErrUndoEmpty {
		t.Fatalf("expected empty undo, got %v", err)
	}
}
