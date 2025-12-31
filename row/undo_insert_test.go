package row

import "testing"

func TestUndoInsert(t *testing.T) {
	store := NewStore(-1)
	log := &UndoLog{}
	t1 := tupleKey(1)
	t2 := tupleKey(2)
	_ = store.Insert(t1)
	log.RecordInsert(t1)
	_ = store.Insert(t2)
	log.RecordInsert(t2)

	if err := log.UndoLast(store); err != nil {
		t.Fatalf("undo last: %v", err)
	}
	if len(store.Rows) != 1 || store.Rows[0] != t1 {
		t.Fatalf("rows=%v", store.Rows)
	}
	if err := log.UndoLast(store); err != nil {
		t.Fatalf("undo last: %v", err)
	}
	if len(store.Rows) != 0 {
		t.Fatalf("rows=%v", store.Rows)
	}
	if err := log.UndoLast(store); err != ErrUndoEmpty {
		t.Fatalf("expected empty undo, got %v", err)
	}
}
