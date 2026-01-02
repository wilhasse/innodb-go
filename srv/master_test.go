package srv

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestMasterSchedulerRunsTasks(t *testing.T) {
	var purgeCount int32
	var flushCount int32
	var statsCount int32

	sched := NewMasterScheduler(MasterConfig{
		PurgeInterval: 10 * time.Millisecond,
		FlushInterval: 15 * time.Millisecond,
		StatsInterval: 20 * time.Millisecond,
		PurgeFn:       func() { atomic.AddInt32(&purgeCount, 1) },
		FlushFn:       func() { atomic.AddInt32(&flushCount, 1) },
		StatsFn:       func() { atomic.AddInt32(&statsCount, 1) },
	})
	if err := sched.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(75 * time.Millisecond)
	if err := sched.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if atomic.LoadInt32(&purgeCount) == 0 {
		t.Fatalf("expected purge task")
	}
	if atomic.LoadInt32(&flushCount) == 0 {
		t.Fatalf("expected flush task")
	}
	if atomic.LoadInt32(&statsCount) == 0 {
		t.Fatalf("expected stats task")
	}
}
