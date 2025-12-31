package buf

import "sync"

// Read-ahead related constants.
const (
	BufReadAheadArea       = 64
	BufReadAheadLinearArea = BufReadAheadArea
	BufReadAheadPendLimit  = 2

	BufReadIbufPagesOnly = 131
	BufReadAnyPage       = 132
)

// ReadAhead tracks sequential access for prefetch decisions.
type ReadAhead struct {
	mu        sync.Mutex
	area      int
	threshold int
	lastPage  map[uint32]uint32
	seqCount  map[uint32]int
}

// NewReadAhead constructs a read-ahead helper.
func NewReadAhead(area, threshold int) *ReadAhead {
	if area <= 0 {
		area = BufReadAheadArea
	}
	if threshold <= 0 || threshold > area {
		threshold = area / 2
		if threshold < 1 {
			threshold = 1
		}
	}
	return &ReadAhead{
		area:      area,
		threshold: threshold,
		lastPage:  make(map[uint32]uint32),
		seqCount:  make(map[uint32]int),
	}
}

// OnAccess returns page IDs to prefetch based on sequential access.
func (r *ReadAhead) OnAccess(space, pageNo uint32) []PageID {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	last, ok := r.lastPage[space]
	if ok && pageNo == last+1 {
		r.seqCount[space]++
	} else {
		r.seqCount[space] = 1
	}
	r.lastPage[space] = pageNo

	if r.seqCount[space] < r.threshold {
		return nil
	}

	r.seqCount[space] = 0
	prefetch := make([]PageID, 0, r.area)
	for i := uint32(1); i <= uint32(r.area); i++ {
		prefetch = append(prefetch, PageID{Space: space, PageNo: pageNo + i})
	}
	return prefetch
}

// Prefetch runs read-ahead and loads pages into the pool.
func (r *ReadAhead) Prefetch(pool *Pool, space, pageNo uint32) []PageID {
	ids := r.OnAccess(space, pageNo)
	if pool == nil || len(ids) == 0 {
		return ids
	}
	for _, id := range ids {
		page, _, err := pool.Fetch(id.Space, id.PageNo)
		if err == nil {
			pool.Release(page)
		}
	}
	return ids
}
