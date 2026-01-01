package lock

import (
	"testing"
	"time"

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
	prev := waitTimeout()
	SetWaitTimeout(200 * time.Millisecond)
	defer SetWaitTimeout(prev)
	trx1 := &trx.Trx{}
	trx2 := &trx.Trx{}
	if _, status := sys.LockTable(trx1, "t1", ModeS); status != LockGranted {
		t.Fatalf("expected first lock granted")
	}
	done := make(chan Status, 1)
	go func() {
		_, status := sys.LockTable(trx2, "t1", ModeX)
		done <- status
	}()
	time.Sleep(20 * time.Millisecond)
	sys.UnlockTable(trx1, "t1")
	if status := waitStatus(t, done, time.Second); status != LockGranted {
		t.Fatalf("expected lock granted after release, got %v", status)
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

func TestLockTableTimeout(t *testing.T) {
	sys := NewLockSys()
	prev := waitTimeout()
	SetWaitTimeout(50 * time.Millisecond)
	defer SetWaitTimeout(prev)
	trx1 := &trx.Trx{}
	trx2 := &trx.Trx{}

	if _, status := sys.LockTable(trx1, "t1", ModeS); status != LockGranted {
		t.Fatalf("expected first lock granted")
	}
	if _, status := sys.LockTable(trx2, "t1", ModeX); status != LockWaitTimeout {
		t.Fatalf("expected lock wait timeout, got %v", status)
	}
}
