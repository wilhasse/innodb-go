package lock

import (
	"testing"
	"time"

	"github.com/wilhasse/innodb-go/trx"
)

func TestLockRecConflict(t *testing.T) {
	sys := NewLockSys()
	prev := waitTimeout()
	SetWaitTimeout(200 * time.Millisecond)
	defer SetWaitTimeout(prev)
	trx1 := &trx.Trx{}
	trx2 := &trx.Trx{}
	rec := RecordKey{Table: "t1", PageNo: 1, HeapNo: 10}

	lock1, status := sys.LockRec(trx1, rec, ModeS)
	if status != LockGranted || lock1 == nil {
		t.Fatalf("expected record lock granted")
	}

	done := make(chan Status, 1)
	go func() {
		_, status := sys.LockRec(trx2, rec, ModeX)
		done <- status
	}()
	time.Sleep(20 * time.Millisecond)
	sys.UnlockRec(trx1, rec)

	if status := waitStatus(t, done, time.Second); status != LockGranted {
		t.Fatalf("expected lock granted after release, got %v", status)
	}
}

func TestLockRecDifferentHeap(t *testing.T) {
	sys := NewLockSys()
	trx1 := &trx.Trx{}
	trx2 := &trx.Trx{}
	rec1 := RecordKey{Table: "t1", PageNo: 1, HeapNo: 10}
	rec2 := RecordKey{Table: "t1", PageNo: 1, HeapNo: 11}

	if _, status := sys.LockRec(trx1, rec1, ModeS); status != LockGranted {
		t.Fatalf("expected first lock granted")
	}
	if _, status := sys.LockRec(trx2, rec2, ModeX); status != LockGranted {
		t.Fatalf("expected different heap lock granted")
	}
}

func TestUnlockRecClearsBits(t *testing.T) {
	sys := NewLockSys()
	trx1 := &trx.Trx{}
	rec1 := RecordKey{Table: "t1", PageNo: 1, HeapNo: 10}
	rec2 := RecordKey{Table: "t1", PageNo: 1, HeapNo: 11}

	lock, status := sys.LockRec(trx1, rec1, ModeS)
	if status != LockGranted || lock == nil {
		t.Fatalf("expected record lock granted")
	}
	lock2, status := sys.LockRec(trx1, rec2, ModeS)
	if status != LockGranted || lock2 != lock {
		t.Fatalf("expected same lock to cover multiple heaps")
	}
	sys.UnlockRec(trx1, rec1)
	if !lock.HasBit(int(rec2.HeapNo)) {
		t.Fatalf("expected remaining heap bit to stay set")
	}
	sys.UnlockRec(trx1, rec2)
	if queue := sys.RecordQueue(rec2); queue != nil && queue.First != nil {
		t.Fatalf("expected queue to be empty after clearing bits")
	}
}

func TestLockRecTimeout(t *testing.T) {
	sys := NewLockSys()
	prev := waitTimeout()
	SetWaitTimeout(50 * time.Millisecond)
	defer SetWaitTimeout(prev)
	trx1 := &trx.Trx{}
	trx2 := &trx.Trx{}
	rec := RecordKey{Table: "t1", PageNo: 1, HeapNo: 10}

	if _, status := sys.LockRec(trx1, rec, ModeX); status != LockGranted {
		t.Fatalf("expected first lock granted")
	}
	if _, status := sys.LockRec(trx2, rec, ModeX); status != LockWaitTimeout {
		t.Fatalf("expected lock wait timeout, got %v", status)
	}
}

func waitStatus(t *testing.T, ch <-chan Status, timeout time.Duration) Status {
	t.Helper()
	select {
	case status := <-ch:
		return status
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for lock status")
		return LockWaitTimeout
	}
}
