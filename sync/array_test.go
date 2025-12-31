package sync

import (
	"testing"
	"time"
)

func TestArrayReserve(t *testing.T) {
	arr := NewArray(2)
	idx1, err := arr.Reserve(1)
	if err != nil {
		t.Fatalf("reserve1: %v", err)
	}
	idx2, err := arr.Reserve(2)
	if err != nil {
		t.Fatalf("reserve2: %v", err)
	}
	if _, err := arr.Reserve(3); err != ErrNoSlot {
		t.Fatalf("expected no slot, got %v", err)
	}
	if err := arr.Release(idx1); err != nil {
		t.Fatalf("release: %v", err)
	}
	idx3, err := arr.Reserve(3)
	if err != nil || idx3 != idx1 {
		t.Fatalf("expected reuse slot, got %d err %v", idx3, err)
	}
	_ = arr.Release(idx2)
	_ = arr.Release(idx3)
}

func TestArrayWaitSignal(t *testing.T) {
	arr := NewArray(1)
	idx, err := arr.Reserve(1)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	done := make(chan struct{})
	go func() {
		_ = arr.Wait(idx)
		close(done)
	}()
	if err := arr.Signal(idx); err != nil {
		t.Fatalf("signal: %v", err)
	}
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("wait timed out")
	}
	_ = arr.Release(idx)
}
