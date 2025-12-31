package sync

import (
	stdsync "sync"
	"sync/atomic"
)

// RWLock wraps sync.RWMutex with counters.
type RWLock struct {
	mu      stdsync.RWMutex
	readers int64
	writers int64
}

// RLock acquires a read lock.
func (l *RWLock) RLock() {
	if l == nil {
		return
	}
	l.mu.RLock()
	atomic.AddInt64(&l.readers, 1)
}

// RUnlock releases a read lock.
func (l *RWLock) RUnlock() {
	if l == nil {
		return
	}
	atomic.AddInt64(&l.readers, -1)
	l.mu.RUnlock()
}

// Lock acquires a write lock.
func (l *RWLock) Lock() {
	if l == nil {
		return
	}
	l.mu.Lock()
	atomic.AddInt64(&l.writers, 1)
}

// Unlock releases a write lock.
func (l *RWLock) Unlock() {
	if l == nil {
		return
	}
	atomic.AddInt64(&l.writers, -1)
	l.mu.Unlock()
}

// ReaderCount reports the current number of readers.
func (l *RWLock) ReaderCount() int64 {
	if l == nil {
		return 0
	}
	return atomic.LoadInt64(&l.readers)
}

// WriterCount reports the current number of writers.
func (l *RWLock) WriterCount() int64 {
	if l == nil {
		return 0
	}
	return atomic.LoadInt64(&l.writers)
}
