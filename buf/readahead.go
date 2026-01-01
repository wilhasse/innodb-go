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
	lastArea  map[uint32]uint32
	areaCount map[uint32]int
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
		lastArea:  make(map[uint32]uint32),
		areaCount: make(map[uint32]int),
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

	area := uint32(0)
	if r.area > 0 {
		area = pageNo / uint32(r.area)
	}
	if lastArea, ok := r.lastArea[space]; ok && lastArea == area {
		r.areaCount[space]++
	} else {
		r.areaCount[space] = 1
	}
	r.lastArea[space] = area

	if r.seqCount[space] < r.threshold {
		if r.areaCount[space] < r.threshold {
			return nil
		}
		r.areaCount[space] = 0
		return r.prefetchArea(space, area)
	}

	r.seqCount[space] = 0
	r.areaCount[space] = 0
	return r.prefetchLinear(space, pageNo)
}

// Prefetch runs read-ahead and loads pages into the pool.
func (r *ReadAhead) Prefetch(pool *Pool, space, pageNo uint32) []PageID {
	ids := r.OnAccess(space, pageNo)
	if pool == nil || len(ids) == 0 {
		return ids
	}
	for _, id := range ids {
		page, _, err := pool.prefetch(id.Space, id.PageNo)
		if err == nil {
			pool.Release(page)
		}
	}
	return ids
}

func (r *ReadAhead) prefetchLinear(space, pageNo uint32) []PageID {
	prefetch := make([]PageID, 0, r.area)
	for i := uint32(1); i <= uint32(r.area); i++ {
		prefetch = append(prefetch, PageID{Space: space, PageNo: pageNo + i})
	}
	return prefetch
}

func (r *ReadAhead) prefetchArea(space, area uint32) []PageID {
	prefetch := make([]PageID, 0, r.area)
	start := area * uint32(r.area)
	for i := uint32(0); i < uint32(r.area); i++ {
		prefetch = append(prefetch, PageID{Space: space, PageNo: start + i})
	}
	return prefetch
}
