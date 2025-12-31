package buf

import "testing"

func TestFlushSinglePage(t *testing.T) {
	pool := NewPool(2, BufPoolDefaultPageSize)
	page, _, err := pool.Fetch(1, 1)
	if err != nil {
		t.Fatalf("unexpected fetch error: %v", err)
	}
	pool.MarkDirty(page)
	pool.Release(page)

	if !pool.FlushPage(PageID{Space: 1, PageNo: 1}) {
		t.Fatalf("expected flush to succeed")
	}
	stats := pool.Stats()
	if stats.Dirty != 0 {
		t.Fatalf("expected no dirty pages after flush")
	}
}

func TestFlushLRULimit(t *testing.T) {
	pool := NewPool(3, BufPoolDefaultPageSize)
	pageA, _, _ := pool.Fetch(1, 1)
	pageB, _, _ := pool.Fetch(1, 2)
	pageC, _, _ := pool.Fetch(1, 3)

	pool.MarkDirty(pageA)
	pool.MarkDirty(pageB)
	pool.Release(pageA)
	pool.Release(pageB)
	pool.Release(pageC)

	_, _, _ = pool.Fetch(1, 2)
	pool.Release(pageB)

	if flushed := pool.FlushLRU(1); flushed != 1 {
		t.Fatalf("expected one page flushed, got %d", flushed)
	}

	stats := pool.Stats()
	if stats.Dirty != 1 {
		t.Fatalf("expected one dirty page remaining, got %d", stats.Dirty)
	}
}

func TestFlushList(t *testing.T) {
	pool := NewPool(2, BufPoolDefaultPageSize)
	pageA, _, _ := pool.Fetch(1, 1)
	pageB, _, _ := pool.Fetch(1, 2)
	pool.MarkDirty(pageA)
	pool.MarkDirty(pageB)
	pool.Release(pageA)
	pool.Release(pageB)

	if flushed := pool.FlushList(0); flushed != 2 {
		t.Fatalf("expected two pages flushed, got %d", flushed)
	}
}
