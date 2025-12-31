package buf

import "testing"

func TestPoolFetchHitMiss(t *testing.T) {
	pool := NewPool(2, BufPoolDefaultPageSize)

	page, hit, err := pool.Fetch(1, 1)
	if err != nil {
		t.Fatalf("unexpected fetch error: %v", err)
	}
	if hit {
		t.Fatalf("expected miss on first fetch")
	}
	pool.Release(page)

	page2, hit, err := pool.Fetch(1, 1)
	if err != nil {
		t.Fatalf("unexpected fetch error: %v", err)
	}
	if !hit || page2 != page {
		t.Fatalf("expected hit on second fetch")
	}
	pool.Release(page2)

	stats := pool.Stats()
	if stats.Hits != 1 || stats.Misses != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestPoolEviction(t *testing.T) {
	pool := NewPool(1, BufPoolDefaultPageSize)

	pageA, _, err := pool.Fetch(1, 1)
	if err != nil {
		t.Fatalf("unexpected fetch error: %v", err)
	}
	pool.Release(pageA)

	_, _, err = pool.Fetch(1, 2)
	if err != nil {
		t.Fatalf("unexpected fetch error: %v", err)
	}

	stats := pool.Stats()
	if stats.Evictions != 1 {
		t.Fatalf("expected one eviction, got %d", stats.Evictions)
	}
}

func TestPoolEvictionPinned(t *testing.T) {
	pool := NewPool(1, BufPoolDefaultPageSize)

	_, _, err := pool.Fetch(1, 1)
	if err != nil {
		t.Fatalf("unexpected fetch error: %v", err)
	}
	if _, _, err := pool.Fetch(1, 2); err != ErrNoFreeFrame {
		t.Fatalf("expected ErrNoFreeFrame, got %v", err)
	}
}

func TestPoolDirtyFlush(t *testing.T) {
	pool := NewPool(2, BufPoolDefaultPageSize)

	page, _, err := pool.Fetch(1, 1)
	if err != nil {
		t.Fatalf("unexpected fetch error: %v", err)
	}
	pool.MarkDirty(page)
	pool.Release(page)

	if flushed := pool.Flush(); flushed != 1 {
		t.Fatalf("expected one flushed page, got %d", flushed)
	}
	stats := pool.Stats()
	if stats.Dirty != 0 {
		t.Fatalf("expected no dirty pages after flush")
	}
}

func TestPoolGetPut(t *testing.T) {
	pool := NewPool(1, BufPoolDefaultPageSize)

	page, hit, err := pool.Get(2, 3)
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if hit {
		t.Fatalf("expected miss on first get")
	}
	pool.MarkDirty(page)
	pool.Put(page)

	stats := pool.Stats()
	if stats.Dirty != 1 {
		t.Fatalf("expected dirty count 1, got %d", stats.Dirty)
	}
}
