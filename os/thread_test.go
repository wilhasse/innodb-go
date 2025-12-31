package os

import (
	"sync/atomic"
	"testing"
)

func TestThreadCreateAndWait(t *testing.T) {
	startCount := atomic.LoadUint64(&ThreadCount)
	handle := ThreadCreate(func(arg any) uint64 {
		return arg.(uint64) + 1
	}, uint64(41))
	if handle == nil || handle.ID == 0 {
		t.Fatalf("expected handle")
	}
	if res := ThreadWait(handle); res != 42 {
		t.Fatalf("result=%d", res)
	}
	if got := atomic.LoadUint64(&ThreadCount); got != startCount {
		t.Fatalf("thread count=%d", got)
	}
}

func TestThreadHelpers(t *testing.T) {
	id := ThreadGetCurrID()
	if id == 0 {
		t.Fatalf("expected current id")
	}
	if !ThreadEq(id, id) {
		t.Fatalf("ThreadEq failed")
	}
	ThreadYield()
	ThreadSleep(1)
}
