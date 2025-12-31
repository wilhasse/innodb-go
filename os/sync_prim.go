package os

import (
	"sync"
	"sync/atomic"
	"time"
)

// SyncMutex is a global mutex used by sync helpers.
var SyncMutex *FastMutex

// EventCount tracks created events.
var EventCount uint64

// FastMutexCount tracks created fast mutexes.
var FastMutexCount uint64

// SyncVarInit resets sync counters and globals.
func SyncVarInit() {
	SyncMutex = nil
	atomic.StoreUint64(&EventCount, 0)
	atomic.StoreUint64(&FastMutexCount, 0)
}

// SyncInit initializes global sync structures.
func SyncInit() {
	SyncVarInit()
	SyncMutex = FastMutexInit()
}

// SyncFree releases global sync structures.
func SyncFree() {
	SyncMutex = nil
}

// Event is a manual-reset event primitive.
type Event struct {
	mu          sync.Mutex
	cond        *sync.Cond
	isSet       bool
	signalCount int64
}

// EventCreate creates a new event.
func EventCreate(_ string) *Event {
	e := &Event{signalCount: 1}
	e.cond = sync.NewCond(&e.mu)
	atomic.AddUint64(&EventCount, 1)
	return e
}

// EventSet signals an event.
func EventSet(e *Event) {
	if e == nil {
		return
	}
	e.mu.Lock()
	if !e.isSet {
		e.isSet = true
		e.signalCount++
	}
	e.cond.Broadcast()
	e.mu.Unlock()
}

// EventReset resets an event and returns the current signal count.
func EventReset(e *Event) int64 {
	if e == nil {
		return 0
	}
	e.mu.Lock()
	e.isSet = false
	sc := e.signalCount
	e.mu.Unlock()
	return sc
}

// EventWaitLow waits for an event to be signaled.
func EventWaitLow(e *Event, resetSigCount int64) {
	if e == nil {
		return
	}
	e.mu.Lock()
	for !e.isSet && (resetSigCount == 0 || e.signalCount == resetSigCount) {
		e.cond.Wait()
	}
	e.mu.Unlock()
}

// EventWaitTime waits for an event to be signaled or times out.
func EventWaitTime(e *Event, timeout time.Duration, resetSigCount int64) bool {
	if e == nil {
		return false
	}
	deadline := time.Now().Add(timeout)
	for {
		e.mu.Lock()
		if e.isSet || (resetSigCount != 0 && e.signalCount != resetSigCount) {
			e.mu.Unlock()
			return true
		}
		e.mu.Unlock()
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// EventFree releases an event.
func EventFree(e *Event) {
	if e == nil {
		return
	}
	atomic.AddUint64(&EventCount, ^uint64(0))
}

// FastMutex provides a simple mutex with TryLock support.
type FastMutex struct {
	mu sync.Mutex
}

// FastMutexInit creates a fast mutex.
func FastMutexInit() *FastMutex {
	atomic.AddUint64(&FastMutexCount, 1)
	return &FastMutex{}
}

// FastMutexLock acquires the mutex.
func FastMutexLock(m *FastMutex) {
	if m == nil {
		return
	}
	m.mu.Lock()
}

// FastMutexUnlock releases the mutex.
func FastMutexUnlock(m *FastMutex) {
	if m == nil {
		return
	}
	m.mu.Unlock()
}

// FastMutexTryLock tries to acquire the mutex.
func FastMutexTryLock(m *FastMutex) bool {
	if m == nil {
		return false
	}
	return m.mu.TryLock()
}

// FastMutexFree releases a fast mutex.
func FastMutexFree(m *FastMutex) {
	if m == nil {
		return
	}
	atomic.AddUint64(&FastMutexCount, ^uint64(0))
}
