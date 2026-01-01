package lock

import (
	"sync/atomic"
	"time"
)

var waitTimeoutNanos int64 = int64(50 * time.Second)

// SetWaitTimeout sets the lock wait timeout duration.
func SetWaitTimeout(d time.Duration) {
	if d < 0 {
		d = 0
	}
	atomic.StoreInt64(&waitTimeoutNanos, int64(d))
}

func waitTimeout() time.Duration {
	return time.Duration(atomic.LoadInt64(&waitTimeoutNanos))
}

func waitForSignal(ch <-chan struct{}, deadline time.Time) bool {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return false
	}
	timer := time.NewTimer(remaining)
	select {
	case <-ch:
		if !timer.Stop() {
			<-timer.C
		}
		return true
	case <-timer.C:
		return false
	}
}
