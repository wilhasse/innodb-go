package log

import "sync/atomic"

// NLogFlushes tracks completed log flushes.
var NLogFlushes uint64

// NPendingLogFlushes tracks pending log flush requests.
var NPendingLogFlushes uint64

func resetMetrics() {
	atomic.StoreUint64(&NLogFlushes, 0)
	atomic.StoreUint64(&NPendingLogFlushes, 0)
}
