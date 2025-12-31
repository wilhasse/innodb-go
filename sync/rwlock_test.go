package sync

import (
	"testing"
	"time"
)

func TestRWLockCounters(t *testing.T) {
	lock := &RWLock{}
	lock.RLock()
	if lock.ReaderCount() != 1 {
		t.Fatalf("readers=%d", lock.ReaderCount())
	}
	lock.RUnlock()
	if lock.ReaderCount() != 0 {
		t.Fatalf("readers=%d", lock.ReaderCount())
	}
	lock.Lock()
	if lock.WriterCount() != 1 {
		t.Fatalf("writers=%d", lock.WriterCount())
	}
	lock.Unlock()
	if lock.WriterCount() != 0 {
		t.Fatalf("writers=%d", lock.WriterCount())
	}
}

func TestRWLockBlocksWriter(t *testing.T) {
	lock := &RWLock{}
	lock.RLock()

	started := make(chan struct{})
	released := make(chan struct{})
	go func() {
		lock.Lock()
		close(started)
		lock.Unlock()
		close(released)
	}()

	select {
	case <-started:
		t.Fatalf("writer should block while reader holds lock")
	case <-time.After(50 * time.Millisecond):
	}

	lock.RUnlock()

	select {
	case <-started:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("writer did not acquire lock")
	}

	select {
	case <-released:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("writer did not release lock")
	}
}
