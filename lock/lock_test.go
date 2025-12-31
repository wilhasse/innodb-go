package lock

import "testing"

func TestLockManagerQueues(t *testing.T) {
	mgr := NewManager()
	l1 := mgr.AcquireTableLock("trx1", "t1", ModeShared)
	l2 := mgr.AcquireTableLock("trx2", "t1", ModeExclusive)
	queue := mgr.TableQueue("t1")
	if queue == nil || queue.First != l1 || queue.Last != l2 {
		t.Fatalf("expected table queue to have two locks")
	}
	if l2.Prev != l1 || l1.Next != l2 {
		t.Fatalf("expected lock linkage")
	}
	mgr.Release(l1)
	if queue.First != l2 || l2.Prev != nil {
		t.Fatalf("expected first lock to be removed")
	}

	rec := RecordKey{Table: "t1", PageNo: 1, RecID: 5}
	r1 := mgr.AcquireRecordLock("trx3", rec, ModeShared)
	rqueue := mgr.RecordQueue(rec)
	if rqueue == nil || rqueue.First != r1 {
		t.Fatalf("expected record queue to contain lock")
	}
	mgr.Release(r1)
	if rqueue.First != nil || rqueue.Last != nil {
		t.Fatalf("expected record queue to be empty")
	}
}

func TestLockSystemLifecycle(t *testing.T) {
	SysCreate(0)
	if Sys() == nil {
		t.Fatalf("expected system to be created")
	}
	SysClose()
	if Sys() != nil {
		t.Fatalf("expected system to be closed")
	}
}

func TestLockGetSize(t *testing.T) {
	if GetSize() <= 0 {
		t.Fatalf("expected lock size to be positive")
	}
}
