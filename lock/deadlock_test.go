package lock

import (
	"testing"
	"time"

	"github.com/wilhasse/innodb-go/trx"
)

func TestDeadlockDetection(t *testing.T) {
	sys := NewLockSys()
	prev := waitTimeout()
	SetWaitTimeout(200 * time.Millisecond)
	defer SetWaitTimeout(prev)
	trx1 := &trx.Trx{}
	trx2 := &trx.Trx{}
	recA := RecordKey{Table: "t1", PageNo: 1, HeapNo: 1}
	recB := RecordKey{Table: "t1", PageNo: 1, HeapNo: 2}

	if _, status := sys.LockRec(trx1, recA, ModeX); status != LockGranted {
		t.Fatalf("expected trx1 lock granted")
	}
	if _, status := sys.LockRec(trx2, recB, ModeX); status != LockGranted {
		t.Fatalf("expected trx2 lock granted")
	}
	waiting := make(chan Status, 1)
	go func() {
		_, status := sys.LockRec(trx1, recB, ModeX)
		waiting <- status
	}()
	time.Sleep(20 * time.Millisecond)
	if _, status := sys.LockRec(trx2, recA, ModeX); status != LockDeadlock {
		t.Fatalf("expected deadlock detection for trx2")
	}
	if sys.deadlock(trx2) {
		t.Fatalf("expected deadlock state cleared for victim")
	}
	sys.UnlockRec(trx2, recB)
	_ = waitStatus(t, waiting, time.Second)
}

func TestNoDeadlockOnSingleWait(t *testing.T) {
	sys := NewLockSys()
	prev := waitTimeout()
	SetWaitTimeout(200 * time.Millisecond)
	defer SetWaitTimeout(prev)
	trx1 := &trx.Trx{}
	trx2 := &trx.Trx{}
	rec := RecordKey{Table: "t1", PageNo: 1, HeapNo: 1}

	if _, status := sys.LockRec(trx1, rec, ModeX); status != LockGranted {
		t.Fatalf("expected trx1 lock granted")
	}
	waiting := make(chan Status, 1)
	go func() {
		_, status := sys.LockRec(trx2, rec, ModeX)
		waiting <- status
	}()
	time.Sleep(20 * time.Millisecond)
	if sys.deadlock(trx2) {
		t.Fatalf("expected no deadlock for single wait")
	}
	sys.UnlockRec(trx1, rec)
	if status := waitStatus(t, waiting, time.Second); status != LockGranted {
		t.Fatalf("expected lock granted after release, got %v", status)
	}
}
