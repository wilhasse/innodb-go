package buf

import "container/list"

// LruOldRatioDefault mirrors the default old ratio.
const LruOldRatioDefault = 37

// LRU maintains the buffer pool LRU list with an old segment.
type LRU struct {
	list     list.List
	oldRatio int
	oldLen   int
}

// NewLRU constructs an LRU list with a given old ratio.
func NewLRU(oldRatio int) *LRU {
	if oldRatio <= 0 || oldRatio >= 100 {
		oldRatio = LruOldRatioDefault
	}
	return &LRU{oldRatio: oldRatio}
}

// SetOldRatio updates the old segment ratio.
func (l *LRU) SetOldRatio(ratio int) {
	if l == nil {
		return
	}
	if ratio <= 0 || ratio >= 100 {
		ratio = LruOldRatioDefault
	}
	l.oldRatio = ratio
	l.Age()
}

// Add inserts a page at the head of the LRU list.
func (l *LRU) Add(page *Page) {
	if l == nil || page == nil {
		return
	}
	page.lruElem = l.list.PushFront(page)
	l.Age()
}

// Touch marks a page as most recently used.
func (l *LRU) Touch(page *Page) {
	if l == nil || page == nil || page.lruElem == nil {
		return
	}
	l.list.MoveToFront(page.lruElem)
	l.Age()
}

// Remove removes a page from the LRU list.
func (l *LRU) Remove(page *Page) {
	if l == nil || page == nil || page.lruElem == nil {
		return
	}
	l.list.Remove(page.lruElem)
	page.lruElem = nil
	page.IsOld = false
	l.Age()
}

// Len returns the total number of pages in the LRU list.
func (l *LRU) Len() int {
	if l == nil {
		return 0
	}
	return l.list.Len()
}

// OldLen returns the current length of the old segment.
func (l *LRU) OldLen() int {
	if l == nil {
		return 0
	}
	return l.oldLen
}

// EvictCandidate returns the least recently used page.
func (l *LRU) EvictCandidate() *Page {
	if l == nil {
		return nil
	}
	e := l.list.Back()
	if e == nil {
		return nil
	}
	return e.Value.(*Page)
}

func (l *LRU) back() *list.Element {
	return l.list.Back()
}

func (l *LRU) prev(e *list.Element) *list.Element {
	if e == nil {
		return nil
	}
	return e.Prev()
}

// Age recomputes the old segment based on the configured ratio.
func (l *LRU) Age() {
	if l == nil {
		return
	}
	total := l.list.Len()
	if total == 0 {
		l.oldLen = 0
		return
	}
	oldLen := total * l.oldRatio / 100
	if oldLen == 0 {
		oldLen = 1
	}
	l.oldLen = oldLen

	count := 0
	for e := l.list.Back(); e != nil; e = e.Prev() {
		page := e.Value.(*Page)
		if count < oldLen {
			page.IsOld = true
		} else {
			page.IsOld = false
		}
		count++
	}
}
