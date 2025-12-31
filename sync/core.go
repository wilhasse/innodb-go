package sync

import (
	stdsync "sync"
	"sync/atomic"
)

// PrimaryWaitArray holds the global wait array.
var PrimaryWaitArray *Array

// Initialized reports whether sync init has run.
var Initialized bool

// MutexSpinWaitCount tracks lock attempts.
var MutexSpinWaitCount int64

// MutexExitCount tracks lock releases.
var MutexExitCount int64

// Init initializes sync primitives.
func Init(waitArraySize int) {
	PrimaryWaitArray = NewArray(waitArraySize)
	Initialized = true
}

// ResetStats resets mutex counters.
func ResetStats() {
	atomic.StoreInt64(&MutexSpinWaitCount, 0)
	atomic.StoreInt64(&MutexExitCount, 0)
}

// SpinMutex is a mutex with basic counters.
type SpinMutex struct {
	mu stdsync.Mutex
}

// Lock acquires the mutex.
func (m *SpinMutex) Lock() {
	atomic.AddInt64(&MutexSpinWaitCount, 1)
	m.mu.Lock()
}

// Unlock releases the mutex.
func (m *SpinMutex) Unlock() {
	atomic.AddInt64(&MutexExitCount, 1)
	m.mu.Unlock()
}
