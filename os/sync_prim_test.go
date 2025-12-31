package os

import (
	"testing"
	"time"
)

func TestEventSetResetWait(t *testing.T) {
	e := EventCreate("evt")
	reset := EventReset(e)
	done := make(chan struct{})
	go func() {
		EventWaitLow(e, reset)
		close(done)
	}()
	time.Sleep(10 * time.Millisecond)
	EventSet(e)
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("event wait timed out")
	}
}

func TestEventWaitTime(t *testing.T) {
	e := EventCreate("evt")
	if EventWaitTime(e, 10*time.Millisecond, 0) {
		t.Fatalf("expected timeout")
	}
	EventSet(e)
	if !EventWaitTime(e, 10*time.Millisecond, 0) {
		t.Fatalf("expected signal")
	}
}

func TestFastMutexTryLock(t *testing.T) {
	m := FastMutexInit()
	FastMutexLock(m)
	if FastMutexTryLock(m) {
		t.Fatalf("expected trylock to fail while locked")
	}
	FastMutexUnlock(m)
	if !FastMutexTryLock(m) {
		t.Fatalf("expected trylock success after unlock")
	}
	FastMutexUnlock(m)
}

func TestSyncInit(t *testing.T) {
	SyncInit()
	if SyncMutex == nil {
		t.Fatalf("expected SyncMutex")
	}
	SyncFree()
}
