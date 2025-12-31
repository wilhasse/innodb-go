package os

import (
	stdos "os"
	"testing"
)

func TestProcVarInit(t *testing.T) {
	UseLargePages = true
	LargePageSize = 4096
	ProcVarInit()
	if UseLargePages || LargePageSize != 0 {
		t.Fatalf("expected vars reset")
	}
}

func TestProcGetNumber(t *testing.T) {
	if ProcGetNumber() == 0 {
		t.Fatalf("expected non-zero pid")
	}
}

func TestMemAllocLarge(t *testing.T) {
	size := uint64(1000)
	buf := MemAllocLarge(&size)
	if buf == nil {
		t.Fatalf("expected buffer")
	}
	page := uint64(stdos.Getpagesize())
	if page == 0 {
		page = 4096
	}
	if size%page != 0 {
		t.Fatalf("expected size aligned, got %d", size)
	}
	if len(buf) != int(size) {
		t.Fatalf("len=%d size=%d", len(buf), size)
	}
	MemFreeLarge(buf)
}
