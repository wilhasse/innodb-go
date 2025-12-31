package os

import (
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// ThreadID identifies a goroutine created via ThreadCreate.
type ThreadID uint64

// ThreadPriority constants mirror os0thread.h.
const (
	ThreadPriorityNone         = 100
	ThreadPriorityBackground   = 1
	ThreadPriorityNormal       = 2
	ThreadPriorityAboveNormal  = 3
)

// ThreadCount tracks active goroutines created by ThreadCreate.
var ThreadCount uint64

// threadIDCounter generates unique thread ids.
var threadIDCounter uint64

// ThreadHandle represents a running goroutine.
type ThreadHandle struct {
	ID     ThreadID
	done   chan struct{}
	result uint64
}

// ThreadFunc defines a goroutine entry point.
type ThreadFunc func(arg any) uint64

// ThreadEq compares thread ids for equality.
func ThreadEq(a, b ThreadID) bool {
	return a == b
}

// ThreadPF converts a thread id to a numeric value.
func ThreadPF(id ThreadID) uint64 {
	return uint64(id)
}

// ThreadGetCurrID returns the current goroutine id.
func ThreadGetCurrID() ThreadID {
	return ThreadID(curGoroutineID())
}

// ThreadGetCurr returns a handle for the current goroutine.
func ThreadGetCurr() ThreadHandle {
	return ThreadHandle{ID: ThreadGetCurrID()}
}

// ThreadCreate starts a goroutine and returns its handle.
func ThreadCreate(start ThreadFunc, arg any) *ThreadHandle {
	if start == nil {
		return nil
	}
	id := ThreadID(atomic.AddUint64(&threadIDCounter, 1))
	atomic.AddUint64(&ThreadCount, 1)
	handle := &ThreadHandle{ID: id, done: make(chan struct{})}
	go func() {
		handle.result = start(arg)
		atomic.AddUint64(&ThreadCount, ^uint64(0))
		close(handle.done)
	}()
	return handle
}

// ThreadWait waits for a goroutine to finish and returns its result.
func ThreadWait(handle *ThreadHandle) uint64 {
	if handle == nil || handle.done == nil {
		return 0
	}
	<-handle.done
	return handle.result
}

// ThreadExit ends the current goroutine.
func ThreadExit() {
	runtime.Goexit()
}

// ThreadYield yields the processor.
func ThreadYield() {
	runtime.Gosched()
}

// ThreadSleep sleeps for at least tm microseconds.
func ThreadSleep(tm uint64) {
	time.Sleep(time.Duration(tm) * time.Microsecond)
}

// ThreadGetPriority returns a placeholder priority value.
func ThreadGetPriority(_ ThreadHandle) uint64 {
	return ThreadPriorityNormal
}

// ThreadSetPriority is a no-op on Go.
func ThreadSetPriority(_ ThreadHandle, _ uint64) {}

// ThreadGetLastError returns 0 (no OS error available).
func ThreadGetLastError() uint64 {
	return 0
}

func curGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	if n <= 0 {
		return 0
	}
	// Stack header: "goroutine 123 ["
	fields := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))
	if len(fields) == 0 {
		return 0
	}
	id, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return 0
	}
	return id
}
