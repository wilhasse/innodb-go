package lock

import (
	"testing"

	"github.com/wilhasse/innodb-go/trx"
)

func TestDeadlockDetection(t *testing.T) {
	sys := NewLockSys()
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
	if _, status := sys.LockRec(trx1, recB, ModeX); status != LockWait {
		t.Fatalf("expected trx1 to wait on recB")
	}
	if _, status := sys.LockRec(trx2, recA, ModeX); status != LockDeadlock {
		t.Fatalf("expected deadlock detection for trx2")
	}
	if sys.deadlock(trx2) {
		t.Fatalf("expected deadlock state cleared for victim")
	}
}

func TestNoDeadlockOnSingleWait(t *testing.T) {
	sys := NewLockSys()
	trx1 := &trx.Trx{}
	trx2 := &trx.Trx{}
	rec := RecordKey{Table: "t1", PageNo: 1, HeapNo: 1}

	if _, status := sys.LockRec(trx1, rec, ModeX); status != LockGranted {
		t.Fatalf("expected trx1 lock granted")
	}
	if _, status := sys.LockRec(trx2, rec, ModeX); status != LockWait {
		t.Fatalf("expected trx2 to wait without deadlock")
	}
	if sys.deadlock(trx2) {
		t.Fatalf("expected no deadlock for single wait")
	}
}
