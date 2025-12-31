package thr

import (
	"runtime"
	"time"
)

// Go starts a goroutine with the provided function.
func Go(fn func()) {
	go fn()
}

// Sleep blocks the current goroutine for at least the duration.
func Sleep(d time.Duration) {
	time.Sleep(d)
}

// Yield yields the processor to allow other goroutines to run.
func Yield() {
	runtime.Gosched()
}
