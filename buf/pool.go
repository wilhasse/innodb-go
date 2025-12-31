package buf

import (
	"container/list"
	"errors"
	"sync"

	"github.com/wilhasse/innodb-go/ut"
)

// ErrNoFreeFrame signals that the buffer pool cannot evict a frame.
var ErrNoFreeFrame = errors.New("buffer pool: no free frame available")

// BufPoolDefaultPageSize mirrors the default page size.
const BufPoolDefaultPageSize = ut.UNIV_PAGE_SIZE

// PageID identifies a page in a tablespace.
type PageID struct {
	Space  uint32
	PageNo uint32
}

// Page represents a buffer pool page frame.
type Page struct {
	ID       PageID
	Data     []byte
	Dirty    bool
	IsOld    bool
	PinCount int
	lruElem  *list.Element
}

// PoolStats holds buffer pool counters.
type PoolStats struct {
	Capacity  int
	Size      int
	Hits      uint64
	Misses    uint64
	Evictions uint64
	Dirty     int
}

// Pool is a simplified buffer pool with LRU eviction.
type Pool struct {
	mu       sync.Mutex
	capacity int
	pageSize int
	pages    map[PageID]*Page
	lru      *LRU
	hits     uint64
	misses   uint64
	evicts   uint64
}

// NewPool constructs a buffer pool with the given capacity and page size.
func NewPool(capacity int, pageSize int) *Pool {
	if capacity < 1 {
		capacity = 1
	}
	if pageSize < 1 {
		pageSize = BufPoolDefaultPageSize
	}
	return &Pool{
		capacity: capacity,
		pageSize: pageSize,
		pages:    make(map[PageID]*Page, capacity),
		lru:      NewLRU(LruOldRatioDefault),
	}
}

// Fetch returns a pinned page frame, loading it if needed.
func (p *Pool) Fetch(space, pageNo uint32) (*Page, bool, error) {
	if p == nil {
		return nil, false, ErrNoFreeFrame
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	id := PageID{Space: space, PageNo: pageNo}
	if page, ok := p.pages[id]; ok {
		page.PinCount++
		p.lru.Touch(page)
		p.hits++
		return page, true, nil
	}

	if len(p.pages) >= p.capacity {
		if !p.evictOne() {
			return nil, false, ErrNoFreeFrame
		}
	}

	page := &Page{
		ID:       id,
		Data:     make([]byte, p.pageSize),
		PinCount: 1,
	}
	p.lru.Add(page)
	p.pages[id] = page
	p.misses++
	return page, false, nil
}

// Get returns a pinned page frame, loading it if needed.
func (p *Pool) Get(space, pageNo uint32) (*Page, bool, error) {
	return p.Fetch(space, pageNo)
}

// Release decreases the pin count of a page.
func (p *Pool) Release(page *Page) {
	if p == nil || page == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if page.PinCount > 0 {
		page.PinCount--
	}
}

// Put releases a page previously returned by Get/Fetch.
func (p *Pool) Put(page *Page) {
	p.Release(page)
}

// MarkDirty marks a page dirty.
func (p *Pool) MarkDirty(page *Page) {
	if p == nil || page == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	page.Dirty = true
}

// Drop removes a page from the pool by id.
func (p *Pool) Drop(space, pageNo uint32) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	id := PageID{Space: space, PageNo: pageNo}
	page := p.pages[id]
	if page == nil {
		return
	}
	delete(p.pages, id)
	if page.lruElem != nil {
		p.lru.Remove(page)
	}
}

// Flush clears dirty flags and returns the number of pages flushed.
func (p *Pool) Flush() int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	flushed := 0
	for _, page := range p.pages {
		if page.Dirty {
			page.Dirty = false
			flushed++
		}
	}
	return flushed
}

// Stats returns the current buffer pool stats.
func (p *Pool) Stats() PoolStats {
	if p == nil {
		return PoolStats{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	dirty := 0
	for _, page := range p.pages {
		if page.Dirty {
			dirty++
		}
	}
	return PoolStats{
		Capacity:  p.capacity,
		Size:      len(p.pages),
		Hits:      p.hits,
		Misses:    p.misses,
		Evictions: p.evicts,
		Dirty:     dirty,
	}
}

func (p *Pool) evictOne() bool {
	for e := p.lru.back(); e != nil; e = p.lru.prev(e) {
		page := e.Value.(*Page)
		if page.PinCount > 0 {
			continue
		}
		delete(p.pages, page.ID)
		p.lru.Remove(page)
		p.evicts++
		return true
	}
	return false
}
