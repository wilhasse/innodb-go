package sync

import (
	"sync/atomic"
	"testing"
)

func TestInitWaitArray(t *testing.T) {
	Init(1)
	if !Initialized {
		t.Fatalf("expected initialized")
	}
	if PrimaryWaitArray == nil {
		t.Fatalf("expected wait array")
	}
	idx, err := PrimaryWaitArray.Reserve(1)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := PrimaryWaitArray.Reserve(2); err != ErrNoSlot {
		t.Fatalf("expected no slot, got %v", err)
	}
	if err := PrimaryWaitArray.Release(idx); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func TestSpinMutexCounters(t *testing.T) {
	ResetStats()
	m := &SpinMutex{}
	m.Lock()
	m.Unlock()
	if got := atomic.LoadInt64(&MutexSpinWaitCount); got != 1 {
		t.Fatalf("spin waits=%d", got)
	}
	if got := atomic.LoadInt64(&MutexExitCount); got != 1 {
		t.Fatalf("exits=%d", got)
	}
	atomic.StoreInt64(&MutexSpinWaitCount, 5)
	atomic.StoreInt64(&MutexExitCount, 7)
	ResetStats()
	if got := atomic.LoadInt64(&MutexSpinWaitCount); got != 0 {
		t.Fatalf("spin waits=%d", got)
	}
	if got := atomic.LoadInt64(&MutexExitCount); got != 0 {
		t.Fatalf("exits=%d", got)
	}
}
