package buf

import "testing"

func TestLRUEvictionOrder(t *testing.T) {
	lru := NewLRU(50)
	p1 := &Page{ID: PageID{Space: 1, PageNo: 1}}
	p2 := &Page{ID: PageID{Space: 1, PageNo: 2}}
	p3 := &Page{ID: PageID{Space: 1, PageNo: 3}}

	lru.Add(p1)
	lru.Add(p2)
	lru.Add(p3)

	if cand := lru.EvictCandidate(); cand != p1 {
		t.Fatalf("expected p1 as eviction candidate")
	}

	lru.Touch(p1)
	if cand := lru.EvictCandidate(); cand != p2 {
		t.Fatalf("expected p2 as eviction candidate after touch")
	}
}

func TestLRUAging(t *testing.T) {
	lru := NewLRU(50)
	p1 := &Page{ID: PageID{Space: 1, PageNo: 1}}
	p2 := &Page{ID: PageID{Space: 1, PageNo: 2}}
	p3 := &Page{ID: PageID{Space: 1, PageNo: 3}}
	p4 := &Page{ID: PageID{Space: 1, PageNo: 4}}

	lru.Add(p1)
	lru.Add(p2)
	lru.Add(p3)
	lru.Add(p4)

	if lru.OldLen() != 2 {
		t.Fatalf("expected old segment length 2, got %d", lru.OldLen())
	}
	if !p1.IsOld || !p2.IsOld {
		t.Fatalf("expected oldest pages to be marked old")
	}
	if p3.IsOld || p4.IsOld {
		t.Fatalf("expected newest pages to be marked young")
	}

	lru.SetOldRatio(25)
	if lru.OldLen() != 1 {
		t.Fatalf("expected old segment length 1 after ratio change")
	}
	if !p1.IsOld {
		t.Fatalf("expected oldest page to remain old")
	}
	if p2.IsOld || p3.IsOld || p4.IsOld {
		t.Fatalf("expected other pages to be young")
	}
}
