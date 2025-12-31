package buf

import "github.com/wilhasse/innodb-go/fil"

// FlushType mirrors buf_flush.
type FlushType int

const (
	BufFlushLRU FlushType = iota
	BufFlushSinglePage
	BufFlushList
)

// FlushPage clears the dirty flag for the given page.
func (p *Pool) FlushPage(id PageID) bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	page, ok := p.pages[id]
	if !ok || !page.Dirty {
		return false
	}
	if err := fil.SpaceWritePage(page.ID.Space, page.ID.PageNo, page.Data); err != nil {
		return false
	}
	page.Dirty = false
	return true
}

// FlushLRU flushes dirty pages starting from the LRU tail.
func (p *Pool) FlushLRU(limit int) int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if limit <= 0 {
		limit = len(p.pages)
	}
	flushed := 0
	for e := p.lru.back(); e != nil && flushed < limit; e = p.lru.prev(e) {
		page := e.Value.(*Page)
		if page.Dirty {
			if err := fil.SpaceWritePage(page.ID.Space, page.ID.PageNo, page.Data); err != nil {
				continue
			}
			page.Dirty = false
			flushed++
		}
	}
	return flushed
}

// FlushList flushes dirty pages from the flush list (LRU in this model).
func (p *Pool) FlushList(limit int) int {
	return p.FlushLRU(limit)
}

// FlushType dispatches flush operations.
func (p *Pool) FlushType(flush FlushType, limit int, id *PageID) int {
	switch flush {
	case BufFlushSinglePage:
		if id == nil {
			return 0
		}
		if p.FlushPage(*id) {
			return 1
		}
		return 0
	case BufFlushList:
		return p.FlushList(limit)
	default:
		return p.FlushLRU(limit)
	}
}
