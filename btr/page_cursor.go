package btr

import (
	"errors"

	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/page"
)

// PageCursor iterates over records in a page-based B-tree.
type PageCursor struct {
	Tree    *PageTree
	pageNo  uint32
	records [][]byte
	index   int
}

// Valid reports whether the cursor is positioned on a record.
func (c *PageCursor) Valid() bool {
	return c != nil && c.Tree != nil && c.index >= 0 && c.index < len(c.records)
}

// Key returns the current record key.
func (c *PageCursor) Key() []byte {
	if !c.Valid() {
		return nil
	}
	key, _ := recordKey(c.records[c.index])
	return key
}

// Value returns the current record value.
func (c *PageCursor) Value() []byte {
	if !c.Valid() {
		return nil
	}
	_, val, ok := decodeLeafRecord(c.records[c.index])
	if !ok {
		return nil
	}
	return val
}

// Next advances to the next record in order.
func (c *PageCursor) Next() bool {
	if !c.Valid() {
		return false
	}
	if c.index+1 < len(c.records) {
		c.index++
		return true
	}
	if c.Tree == nil {
		c.invalidate()
		return false
	}
	next, err := c.Tree.leafNextPage(c.pageNo)
	if err != nil {
		c.invalidate()
		return false
	}
	cur, err := c.Tree.cursorFromPageIndex(next, 0, true)
	if err != nil || cur == nil {
		c.invalidate()
		return false
	}
	*c = *cur
	return c.Valid()
}

func (c *PageCursor) invalidate() {
	if c == nil {
		return
	}
	c.records = nil
	c.index = -1
}

// First positions a page cursor on the first record in key order.
func (t *PageTree) First() (*PageCursor, error) {
	if t == nil {
		return nil, errors.New("btr: nil tree")
	}
	if t.RootPage == fil.NullPageOffset {
		return nil, nil
	}
	if err := t.ensureRootInitialized(); err != nil {
		return nil, err
	}
	start, err := t.leftmostLeaf()
	if err != nil {
		return nil, err
	}
	return t.cursorFromPageIndex(start, 0, true)
}

// Seek positions a page cursor based on the search key and mode.
func (t *PageTree) Seek(key []byte, mode SearchMode) (*PageCursor, bool, error) {
	if t == nil {
		return nil, false, errors.New("btr: nil tree")
	}
	if t.RootPage == fil.NullPageOffset {
		return nil, false, nil
	}
	if err := t.ensureRootInitialized(); err != nil {
		return nil, false, err
	}
	pageNo := t.RootPage
	for {
		h, err := t.fetchPage(pageNo)
		if err != nil {
			return nil, false, err
		}
		level := page.PageGetLevel(h.data)
		records := collectUserRecords(h.data)
		if level == 0 {
			idx, exact := findRecordIndex(records, key, t.Compare)
			prev := page.PageGetPrev(h.data)
			next := page.PageGetNext(h.data)
			_ = h.commit(false)
			switch mode {
			case SearchLE:
				if exact {
					cur, err := t.cursorFromPageIndex(pageNo, idx, false)
					return cur, true, err
				}
				idx--
				cur, err := t.cursorFromPageIndex(pageNo, idx, false)
				if cur == nil && !isNullPageNo(prev) {
					cur, err = t.cursorFromPageIndex(prev, -1, false)
				}
				return cur, false, err
			default:
				cur, err := t.cursorFromPageIndex(pageNo, idx, true)
				if cur == nil && !isNullPageNo(next) {
					cur, err = t.cursorFromPageIndex(next, 0, true)
				}
				return cur, exact, err
			}
		}
		child, ok := findChildPage(records, key, t.Compare)
		_ = h.commit(false)
		if !ok {
			return nil, false, errors.New("btr: missing child page")
		}
		pageNo = child
	}
}

func (t *PageTree) cursorFromPageIndex(pageNo uint32, idx int, forward bool) (*PageCursor, error) {
	for !isNullPageNo(pageNo) {
		records, prev, next, err := t.leafRecords(pageNo)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			if forward {
				pageNo = next
			} else {
				pageNo = prev
			}
			idx = 0
			continue
		}
		if forward {
			if idx < 0 {
				idx = 0
			}
			if idx >= len(records) {
				pageNo = next
				idx = 0
				continue
			}
		} else {
			if idx >= len(records) {
				idx = len(records) - 1
			}
			if idx < 0 {
				pageNo = prev
				idx = -1
				continue
			}
		}
		return &PageCursor{Tree: t, pageNo: pageNo, records: records, index: idx}, nil
	}
	return nil, nil
}

func (t *PageTree) leafRecords(pageNo uint32) ([][]byte, uint32, uint32, error) {
	if t == nil {
		return nil, 0, 0, errors.New("btr: nil tree")
	}
	if isNullPageNo(pageNo) {
		return nil, fil.NullPageOffset, fil.NullPageOffset, nil
	}
	h, err := t.fetchPage(pageNo)
	if err != nil {
		return nil, 0, 0, err
	}
	level := page.PageGetLevel(h.data)
	if level != 0 {
		_ = h.commit(false)
		return nil, 0, 0, errors.New("btr: expected leaf page")
	}
	records := collectUserRecords(h.data)
	prev := page.PageGetPrev(h.data)
	next := page.PageGetNext(h.data)
	_ = h.commit(false)
	return records, prev, next, nil
}

func (t *PageTree) leafNextPage(pageNo uint32) (uint32, error) {
	_, _, next, err := t.leafRecords(pageNo)
	return next, err
}
