package trx

import (
	"testing"

	"github.com/wilhasse/innodb-go/read"
	"github.com/wilhasse/innodb-go/ut"
)

func TestPurgeArrayStoreAndBiggest(t *testing.T) {
	arr := NewPurgeArray(1)
	first := arr.Store(10, 1)
	if first == nil || arr.Used != 1 {
		t.Fatalf("expected used=1")
	}
	arr.Store(9, 7)
	if arr.Used != 2 {
		t.Fatalf("expected used=2")
	}
	trxID, undoNo, ok := arr.Biggest()
	if !ok || trxID != 10 || undoNo != 1 {
		t.Fatalf("biggest=%d/%d ok=%v", trxID, undoNo, ok)
	}
	arr.Store(10, 5)
	trxID, undoNo, ok = arr.Biggest()
	if !ok || trxID != 10 || undoNo != 5 {
		t.Fatalf("biggest=%d/%d ok=%v", trxID, undoNo, ok)
	}
	arr.Remove(first)
	if arr.Used != 2 {
		t.Fatalf("expected used=2")
	}
}

func TestPurgeFetchRelease(t *testing.T) {
	PurgeVarInit()
	PurgeSysCreate()

	PurgeAddUpdateUndoToHistory(1, 2)
	rec, info := PurgeFetchNextRec()
	if rec == nil || info == nil {
		t.Fatalf("expected record and info")
	}
	if PurgeSys.Arr == nil || PurgeSys.Arr.Used != 1 {
		t.Fatalf("expected used=1")
	}
	PurgeRecRelease(info)
	if PurgeSys.Arr.Used != 0 {
		t.Fatalf("expected used=0")
	}
}

func TestPurgeRunLimit(t *testing.T) {
	PurgeVarInit()
	PurgeSysCreate()

	PurgeSys.HandleLimit = ut.Ulint(2)
	PurgeAddUpdateUndoToHistory(1, 1)
	PurgeAddUpdateUndoToHistory(2, 1)
	PurgeAddUpdateUndoToHistory(3, 1)

	handled := PurgeRun()
	if handled != 2 {
		t.Fatalf("handled=%d", handled)
	}
	if len(PurgeSys.Queue) != 1 {
		t.Fatalf("queue=%d", len(PurgeSys.Queue))
	}
	if PurgeSys.PagesHandled != ut.Ulint(2) {
		t.Fatalf("pages=%d", PurgeSys.PagesHandled)
	}

	PurgeSys.HandleLimit = 0
	handled = PurgeRun()
	if handled != 1 {
		t.Fatalf("handled=%d", handled)
	}
	if len(PurgeSys.Queue) != 0 {
		t.Fatalf("queue=%d", len(PurgeSys.Queue))
	}
	if PurgeSys.PagesHandled != ut.Ulint(3) {
		t.Fatalf("pages=%d", PurgeSys.PagesHandled)
	}
}

func TestPurgeUpdateUndoMustExist(t *testing.T) {
	PurgeVarInit()
	PurgeSysCreate()

	view := read.NewReadView(10, []uint64{11, 12})
	PurgeSys.View = view

	if !PurgeUpdateUndoMustExist(12) {
		t.Fatalf("expected undo to exist")
	}
	if PurgeUpdateUndoMustExist(5) {
		t.Fatalf("expected undo not required")
	}
}
