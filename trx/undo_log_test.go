package trx

import "testing"

func TestUndoLogAppendPopAndReset(t *testing.T) {
	log := NewUndoLog(1, UndoLogUpdate)
	log.Append(UndoRecord{UndoNo: 10})
	log.Append(UndoRecord{UndoNo: 20})

	last, ok := log.Last()
	if !ok || last.UndoNo != 20 {
		t.Fatalf("last=%d ok=%v", last.UndoNo, ok)
	}
	prev, ok := log.Prev(1)
	if !ok || prev.UndoNo != 10 {
		t.Fatalf("prev=%d ok=%v", prev.UndoNo, ok)
	}
	rec, ok := log.Pop()
	if !ok || rec.UndoNo != 20 || len(log.Records) != 1 {
		t.Fatalf("pop=%d ok=%v len=%d", rec.UndoNo, ok, len(log.Records))
	}

	log.Reset(3)
	if log.TrxID != 3 || len(log.Records) != 0 {
		t.Fatalf("reset id=%d len=%d", log.TrxID, len(log.Records))
	}
}

func TestUndoLogPrevBounds(t *testing.T) {
	log := NewUndoLog(1, UndoLogInsert)
	log.Append(UndoRecord{UndoNo: 1})
	if _, ok := log.Prev(0); ok {
		t.Fatalf("expected invalid prev index")
	}
}
