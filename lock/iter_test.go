package lock

import "testing"

func TestQueueIteratorResetTable(t *testing.T) {
	lock := &Lock{Type: LockTable}
	iter := &QueueIterator{}
	iter.Reset(lock, UndefinedBitNo)
	if iter.Current != lock {
		t.Fatalf("expected current to be set")
	}
	if iter.BitNo != UndefinedBitNo {
		t.Fatalf("expected bit no to be undefined")
	}
}

func TestQueueIteratorResetRec(t *testing.T) {
	lock := &Lock{Type: LockRec, Bits: []bool{false, true, false}}
	iter := &QueueIterator{}
	iter.Reset(lock, UndefinedBitNo)
	if iter.BitNo != 1 {
		t.Fatalf("expected bit no 1, got %d", iter.BitNo)
	}
}

func TestQueueIteratorPrev(t *testing.T) {
	first := &Lock{Type: LockTable}
	second := &Lock{Type: LockTable, Prev: first}
	third := &Lock{Type: LockTable, Prev: second}
	iter := &QueueIterator{Current: third}

	if prev := iter.GetPrev(); prev != second {
		t.Fatalf("expected second lock")
	}
	if prev := iter.GetPrev(); prev != first {
		t.Fatalf("expected first lock")
	}
	if prev := iter.GetPrev(); prev != nil {
		t.Fatalf("expected nil at start of queue")
	}
}
