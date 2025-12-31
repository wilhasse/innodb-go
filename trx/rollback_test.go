package trx

import "testing"

func TestRollbackToSavepoint(t *testing.T) {
	trx := &Trx{}
	values := []int{}

	for i := 0; i < 2; i++ {
		values = append(values, i)
		RecordUndo(trx, func() {
			values = values[:len(values)-1]
		})
	}
	savept := SavepointTake(trx)

	values = append(values, 2)
	RecordUndo(trx, func() {
		values = values[:len(values)-1]
	})

	rolled := RollbackToSavepoint(trx, savept)
	if rolled != 1 {
		t.Fatalf("rolled=%d", rolled)
	}
	if len(values) != 2 {
		t.Fatalf("values=%v", values)
	}
	if len(trx.UndoLog) != savept.UndoLen {
		t.Fatalf("undo len=%d", len(trx.UndoLog))
	}
	if len(trx.Savepoints) != 1 {
		t.Fatalf("savepoints=%d", len(trx.Savepoints))
	}

	rolled = Rollback(trx)
	if rolled != 2 {
		t.Fatalf("rolled=%d", rolled)
	}
	if len(values) != 0 {
		t.Fatalf("values=%v", values)
	}
	if len(trx.UndoLog) != 0 {
		t.Fatalf("undo len=%d", len(trx.UndoLog))
	}
	if len(trx.Savepoints) != 0 {
		t.Fatalf("savepoints=%d", len(trx.Savepoints))
	}
}
