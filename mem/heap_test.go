package mem

import (
	"bytes"
	"testing"
)

func TestHeapAllocGrowth(t *testing.T) {
	h := HeapCreate(32)
	if len(h.blocks) != 1 {
		t.Fatalf("expected 1 block, got %d", len(h.blocks))
	}
	_ = h.Alloc(16)
	_ = h.Alloc(10)
	if len(h.blocks) != 1 {
		t.Fatalf("expected 1 block after small allocs, got %d", len(h.blocks))
	}
	_ = h.Alloc(16)
	if len(h.blocks) != 2 {
		t.Fatalf("expected 2 blocks after growth, got %d", len(h.blocks))
	}
	if h.Size() != len(h.blocks[0].buf)+len(h.blocks[1].buf) {
		t.Fatalf("Size mismatch: %d", h.Size())
	}
}

func TestHeapAllocZero(t *testing.T) {
	pool := NewBufferPool(64)
	pool.pool.New = func() any {
		buf := make([]byte, 64)
		for i := range buf {
			buf[i] = 0xAA
		}
		return buf
	}
	h := NewHeapWithPool(64, HeapDynamic, pool)
	buf := h.AllocZero(8)
	for i, b := range buf {
		if b != 0 {
			t.Fatalf("expected zero at %d, got %#x", i, b)
		}
	}
}

func TestHeapHelpers(t *testing.T) {
	h := HeapCreate(64)
	if got := h.Dup([]byte("hi")); string(got) != "hi" {
		t.Fatalf("Dup=%q", got)
	}
	if got := h.StrDup("ok"); !bytes.Equal(got, []byte{'o', 'k', 0}) {
		t.Fatalf("StrDup=%v", got)
	}
	if got := h.StrDupl("hello", 2); !bytes.Equal(got, []byte{'h', 'e', 0}) {
		t.Fatalf("StrDupl=%v", got)
	}
	if got := h.StrCat("a", "b"); !bytes.Equal(got, []byte{'a', 'b', 0}) {
		t.Fatalf("StrCat=%v", got)
	}
	if got := h.Printf("x=%d", 7); !bytes.Equal(got, []byte{'x', '=', '7', 0}) {
		t.Fatalf("Printf=%v", got)
	}
}

func TestHeapTopFree(t *testing.T) {
	h := HeapCreate(64)
	a := h.Alloc(4)
	b := h.Alloc(4)
	top := h.GetTop(4)
	if top == nil || &top[0] != &b[0] {
		t.Fatalf("GetTop did not return latest allocation")
	}
	h.FreeTop(4)
	top = h.GetTop(4)
	if top == nil || &top[0] != &a[0] {
		t.Fatalf("FreeTop did not rewind allocation")
	}
}

func TestHeapReset(t *testing.T) {
	h := HeapCreate(32)
	_ = h.Alloc(16)
	_ = h.Alloc(32)
	if len(h.blocks) < 2 {
		t.Fatalf("expected growth before reset")
	}
	h.Reset()
	if len(h.blocks) != 1 {
		t.Fatalf("expected single block after reset, got %d", len(h.blocks))
	}
	if h.blocks[0].used != 0 {
		t.Fatalf("expected used reset to 0, got %d", h.blocks[0].used)
	}
}

func TestHeapUsesPool(t *testing.T) {
	pool := NewBufferPool(32)
	pool.pool.New = func() any {
		buf := make([]byte, 32)
		buf[0] = 0xCC
		return buf
	}
	h := NewHeapWithPool(32, HeapDynamic, pool)
	if h.blocks[0].buf[0] != 0xCC {
		t.Fatalf("expected heap to allocate from pool")
	}
}

func TestBufferPoolBasics(t *testing.T) {
	pool := NewBufferPool(16)
	buf := pool.Get()
	if len(buf) != 16 {
		t.Fatalf("expected len 16, got %d", len(buf))
	}
	buf[0] = 0x5A
	pool.Put(buf)
	_ = pool.Get()
}
