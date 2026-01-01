package lock

import (
	"testing"

	"github.com/wilhasse/innodb-go/trx"
)

func TestLockTableGrantAndUnlock(t *testing.T) {
	sys := NewLockSys()
	trx1 := &trx.Trx{}
	lock, status := sys.LockTable(trx1, "t1", ModeS)
	if status != LockGranted || lock == nil {
		t.Fatalf("expected lock granted")
	}
	sys.UnlockTable(trx1, "t1")
	if queue := sys.TableQueue("t1"); queue != nil && queue.First != nil {
		t.Fatalf("expected queue to be empty after unlock")
	}
	trx2 := &trx.Trx{}
	lock, status = sys.LockTable(trx2, "t1", ModeX)
	if status != LockGranted || lock == nil {
		t.Fatalf("expected lock granted after unlock")
	}
}

func TestLockTableConflict(t *testing.T) {
	sys := NewLockSys()
	trx1 := &trx.Trx{}
	trx2 := &trx.Trx{}
	if _, status := sys.LockTable(trx1, "t1", ModeS); status != LockGranted {
		t.Fatalf("expected first lock granted")
	}
	lock, status := sys.LockTable(trx2, "t1", ModeX)
	if status != LockWait || lock == nil || lock.Flags&FlagWait == 0 {
		t.Fatalf("expected conflicting lock to wait")
	}
	queue := sys.TableQueue("t1")
	if queue == nil || queue.First == nil || queue.Last != lock {
		t.Fatalf("expected waiter appended to queue")
	}
}

func TestLockTableUpgrade(t *testing.T) {
	sys := NewLockSys()
	trx1 := &trx.Trx{}
	lock, status := sys.LockTable(trx1, "t1", ModeIS)
	if status != LockGranted || lock == nil {
		t.Fatalf("expected lock granted")
	}
	lock2, status := sys.LockTable(trx1, "t1", ModeIX)
	if status != LockGranted || lock2 != lock {
		t.Fatalf("expected upgrade on same lock")
	}
	if lock.Mode != ModeIX {
		t.Fatalf("expected mode upgrade to IX")
	}
}
