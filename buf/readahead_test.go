package buf

import "testing"

func TestReadAheadSequential(t *testing.T) {
	ra := NewReadAhead(4, 3)

	if ids := ra.OnAccess(1, 1); len(ids) != 0 {
		t.Fatalf("expected no prefetch on first access")
	}
	if ids := ra.OnAccess(1, 2); len(ids) != 0 {
		t.Fatalf("expected no prefetch on second access")
	}
	ids := ra.OnAccess(1, 3)
	if len(ids) != 4 {
		t.Fatalf("expected prefetch of 4 pages, got %d", len(ids))
	}
	if ids[0].PageNo != 4 || ids[len(ids)-1].PageNo != 7 {
		t.Fatalf("unexpected prefetch range: %+v", ids)
	}
}

func TestReadAheadResetOnGap(t *testing.T) {
	ra := NewReadAhead(4, 3)

	_ = ra.OnAccess(1, 1)
	_ = ra.OnAccess(1, 2)
	ids := ra.OnAccess(1, 4)
	if len(ids) != 0 {
		t.Fatalf("expected no prefetch after gap")
	}
}

func TestReadAheadPrefetchLoadsPool(t *testing.T) {
	pool := NewPool(4, BufPoolDefaultPageSize)
	ra := NewReadAhead(2, 2)

	if ids := ra.Prefetch(pool, 1, 1); len(ids) != 0 {
		t.Fatalf("expected no prefetch yet")
	}
	ids := ra.Prefetch(pool, 1, 2)
	if len(ids) != 2 {
		t.Fatalf("expected prefetch of 2 pages")
	}
	stats := pool.Stats()
	if stats.Size != 2 {
		t.Fatalf("expected prefetched pages to be in pool")
	}
}

func TestReadAheadRandom(t *testing.T) {
	ra := NewReadAhead(4, 3)

	if ids := ra.OnAccess(1, 1); len(ids) != 0 {
		t.Fatalf("expected no prefetch on first access")
	}
	if ids := ra.OnAccess(1, 3); len(ids) != 0 {
		t.Fatalf("expected no prefetch on second access")
	}
	ids := ra.OnAccess(1, 2)
	if len(ids) != 4 {
		t.Fatalf("expected random prefetch of 4 pages, got %d", len(ids))
	}
	if ids[0].PageNo != 0 || ids[len(ids)-1].PageNo != 3 {
		t.Fatalf("unexpected random prefetch range: %+v", ids)
	}
}
