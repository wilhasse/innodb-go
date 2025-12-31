package trx

import "testing"

func TestRollbackSegmentLifecycle(t *testing.T) {
	RsegVarInit()

	rseg := RsegCreate(1, 2)
	if rseg == nil {
		t.Fatalf("expected rseg")
	}
	if got := RsegGetOnID(1); got != rseg {
		t.Fatalf("expected rseg lookup")
	}

	if !rseg.AddUpdateUndo(UndoRecord{Type: UndoUpdExistRec}) {
		t.Fatalf("expected update undo added")
	}
	if !rseg.AddInsertUndo(UndoRecord{Type: UndoInsertRec}) {
		t.Fatalf("expected insert undo added")
	}
	if rseg.AddInsertUndo(UndoRecord{Type: UndoInsertRec}) {
		t.Fatalf("expected max size enforcement")
	}

	rseg.CacheUpdateUndo(UndoRecord{Type: UndoUpdDelRec})
	rseg.CacheInsertUndo(UndoRecord{Type: UndoDelMarkRec})
	if _, ok := rseg.PopCachedUpdateUndo(); !ok {
		t.Fatalf("expected cached update undo")
	}
	if _, ok := rseg.PopCachedInsertUndo(); !ok {
		t.Fatalf("expected cached insert undo")
	}

	RsegFree(rseg)
	if got := RsegGetOnID(1); got != nil {
		t.Fatalf("expected rseg freed")
	}
}
