package btr

import (
	"bytes"
	"errors"

	"github.com/wilhasse/innodb-go/buf"
	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/fsp"
	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/ut"
)

// PageTree stores B-tree records directly in page bytes.
type PageTree struct {
	SpaceID  uint32
	RootPage uint32
	Compare  CompareFunc
	MaxRecs  int
	size     int
}

// NewPageTree creates a page-based B-tree for the given space.
func NewPageTree(spaceID uint32, compare CompareFunc) *PageTree {
	if compare == nil {
		compare = bytes.Compare
	}
	return &PageTree{
		SpaceID:  spaceID,
		RootPage: fil.NullPageOffset,
		Compare:  compare,
		MaxRecs:  PageMaxRecords,
	}
}

// Size returns the number of stored user records.
func (t *PageTree) Size() int {
	if t == nil {
		return 0
	}
	return t.size
}

// Insert adds or replaces a key/value pair.
func (t *PageTree) Insert(key, value []byte) (bool, error) {
	if t == nil {
		return false, errors.New("btr: nil tree")
	}
	t.ensureDefaults()
	if t.RootPage == fil.NullPageOffset {
		root, err := t.allocPage(0)
		if err != nil {
			return false, err
		}
		t.RootPage = root
	}

	split, sepKey, rightPage, replaced, err := t.insertPage(t.RootPage, key, value)
	if err != nil {
		return false, err
	}
	if !split {
		return replaced, nil
	}

	rootLevel, err := t.pageLevel(t.RootPage)
	if err != nil {
		return false, err
	}
	newRoot, err := t.allocPage(rootLevel + 1)
	if err != nil {
		return false, err
	}
	leftKey, err := t.pageMinKey(t.RootPage)
	if err != nil {
		return false, err
	}
	if len(leftKey) == 0 {
		leftKey = sepKey
	}
	rightKey := sepKey
	if len(rightKey) == 0 {
		rightKey, _ = t.pageMinKey(rightPage)
	}

	h, err := t.fetchPage(newRoot)
	if err != nil {
		return false, err
	}
	records := [][]byte{
		encodeNodePtrRecord(leftKey, t.RootPage),
		encodeNodePtrRecord(rightKey, rightPage),
	}
	if !rebuildIndexPage(h.data, newRoot, rootLevel+1, fil.NullPageOffset, fil.NullPageOffset, records) {
		_ = h.commit(false)
		return false, errors.New("btr: root rebuild failed")
	}
	if err := h.commit(true); err != nil {
		return false, err
	}
	t.RootPage = newRoot
	return replaced, nil
}

// Search looks up a key in the page-based tree.
func (t *PageTree) Search(key []byte) ([]byte, bool, error) {
	if t == nil {
		return nil, false, errors.New("btr: nil tree")
	}
	if t.RootPage == fil.NullPageOffset {
		return nil, false, nil
	}
	t.ensureDefaults()

	pageNo := t.RootPage
	for {
		h, err := t.fetchPage(pageNo)
		if err != nil {
			return nil, false, err
		}
		level := page.PageGetLevel(h.data)
		if level == 0 {
			records := collectUserRecords(h.data)
			for _, recBytes := range records {
				recKey, ok := recordKey(recBytes)
				if !ok {
					continue
				}
				cmp := t.Compare(recKey, key)
				if cmp == 0 {
					_, val, ok := decodeLeafRecord(recBytes)
					_ = h.commit(false)
					return val, ok, nil
				}
				if cmp > 0 {
					break
				}
			}
			_ = h.commit(false)
			return nil, false, nil
		}
		records := collectUserRecords(h.data)
		child, ok := findChildPage(records, key, t.Compare)
		_ = h.commit(false)
		if !ok {
			return nil, false, errors.New("btr: missing child page")
		}
		pageNo = child
	}
}

func (t *PageTree) ensureDefaults() {
	if t.Compare == nil {
		t.Compare = bytes.Compare
	}
	if t.MaxRecs <= 0 {
		t.MaxRecs = PageMaxRecords
	}
}

func (t *PageTree) maxRecords() int {
	if t.MaxRecs <= 0 {
		return PageMaxRecords
	}
	return t.MaxRecs
}

type pageHandle struct {
	spaceID uint32
	pageNo  uint32
	data    []byte
	pool    *buf.Pool
	bufPage *buf.Page
}

func (t *PageTree) fetchPage(pageNo uint32) (*pageHandle, error) {
	pool := buf.GetPool(t.SpaceID, pageNo)
	if pool != nil {
		bufPage, _, err := pool.Fetch(t.SpaceID, pageNo)
		if err != nil {
			return nil, err
		}
		return &pageHandle{
			spaceID: t.SpaceID,
			pageNo:  pageNo,
			data:    bufPage.Data,
			pool:    pool,
			bufPage: bufPage,
		}, nil
	}
	pageBytes, err := fil.SpaceReadPage(t.SpaceID, pageNo)
	if err != nil {
		return nil, err
	}
	if pageBytes == nil {
		pageBytes = make([]byte, ut.UNIV_PAGE_SIZE)
	}
	return &pageHandle{spaceID: t.SpaceID, pageNo: pageNo, data: pageBytes}, nil
}

func (h *pageHandle) commit(dirty bool) error {
	if h == nil {
		return nil
	}
	if h.pool != nil && h.bufPage != nil {
		if dirty {
			h.pool.MarkDirty(h.bufPage)
		}
		h.pool.Release(h.bufPage)
		return nil
	}
	if dirty {
		return fil.SpaceWritePage(h.spaceID, h.pageNo, h.data)
	}
	return nil
}

func (t *PageTree) allocPage(level uint16) (uint32, error) {
	pageNo := fsp.AllocPage(t.SpaceID)
	if pageNo == fil.NullPageOffset {
		return fil.NullPageOffset, errors.New("btr: no free page")
	}
	h, err := t.fetchPage(pageNo)
	if err != nil {
		return fil.NullPageOffset, err
	}
	if !initIndexPageBytes(h.data, pageNo, level) {
		_ = h.commit(false)
		return fil.NullPageOffset, errors.New("btr: init page failed")
	}
	if err := h.commit(true); err != nil {
		return fil.NullPageOffset, err
	}
	return pageNo, nil
}

func (t *PageTree) pageLevel(pageNo uint32) (uint16, error) {
	h, err := t.fetchPage(pageNo)
	if err != nil {
		return 0, err
	}
	level := page.PageGetLevel(h.data)
	_ = h.commit(false)
	return level, nil
}

func (t *PageTree) pageMinKey(pageNo uint32) ([]byte, error) {
	h, err := t.fetchPage(pageNo)
	if err != nil {
		return nil, err
	}
	records := collectUserRecords(h.data)
	_ = h.commit(false)
	if len(records) == 0 {
		return nil, nil
	}
	key, ok := recordKey(records[0])
	if !ok {
		return nil, nil
	}
	return key, nil
}

func (t *PageTree) insertPage(pageNo uint32, key, value []byte) (bool, []byte, uint32, bool, error) {
	h, err := t.fetchPage(pageNo)
	if err != nil {
		return false, nil, fil.NullPageOffset, false, err
	}
	pageBytes := h.data
	level := page.PageGetLevel(pageBytes)
	if level == 0 {
		records := collectUserRecords(pageBytes)
		idx, exact := findRecordIndex(records, key, t.Compare)
		if exact {
			records[idx] = encodeLeafRecord(key, value)
		} else {
			records = insertRecord(records, idx, encodeLeafRecord(key, value))
			t.size++
		}
		if len(records) <= t.maxRecords() {
			prev := page.PageGetPrev(pageBytes)
			next := page.PageGetNext(pageBytes)
			if !rebuildIndexPage(pageBytes, pageNo, level, prev, next, records) {
				_ = h.commit(false)
				return false, nil, fil.NullPageOffset, exact, errors.New("btr: leaf rebuild failed")
			}
			if err := h.commit(true); err != nil {
				return false, nil, fil.NullPageOffset, exact, err
			}
			return false, nil, fil.NullPageOffset, exact, nil
		}

		mid := len(records) / 2
		leftRecords := records[:mid]
		rightRecords := records[mid:]
		rightPage, err := t.allocPage(0)
		if err != nil {
			_ = h.commit(false)
			return false, nil, fil.NullPageOffset, exact, err
		}
		sepKey := recordKeyOrEmpty(rightRecords)
		prev := page.PageGetPrev(pageBytes)
		next := page.PageGetNext(pageBytes)
		if !rebuildIndexPage(pageBytes, pageNo, 0, prev, rightPage, leftRecords) {
			_ = h.commit(false)
			return false, nil, fil.NullPageOffset, exact, errors.New("btr: leaf split rebuild failed")
		}
		if err := h.commit(true); err != nil {
			return false, nil, fil.NullPageOffset, exact, err
		}

		rh, err := t.fetchPage(rightPage)
		if err != nil {
			return false, nil, fil.NullPageOffset, exact, err
		}
		if !rebuildIndexPage(rh.data, rightPage, 0, pageNo, next, rightRecords) {
			_ = rh.commit(false)
			return false, nil, fil.NullPageOffset, exact, errors.New("btr: leaf split right rebuild failed")
		}
		if err := rh.commit(true); err != nil {
			return false, nil, fil.NullPageOffset, exact, err
		}
		if !isNullPageNo(next) {
			nh, err := t.fetchPage(next)
			if err != nil {
				return false, nil, fil.NullPageOffset, exact, err
			}
			page.PageSetPrev(nh.data, rightPage)
			if err := nh.commit(true); err != nil {
				return false, nil, fil.NullPageOffset, exact, err
			}
		}
		return true, sepKey, rightPage, exact, nil
	}

	records := collectUserRecords(pageBytes)
	child, ok := findChildPage(records, key, t.Compare)
	if !ok {
		_ = h.commit(false)
		return false, nil, fil.NullPageOffset, false, errors.New("btr: no child to descend")
	}
	split, sepKey, rightPage, replaced, err := t.insertPage(child, key, value)
	if err != nil {
		_ = h.commit(false)
		return false, nil, fil.NullPageOffset, replaced, err
	}
	if !split {
		_ = h.commit(false)
		return false, nil, fil.NullPageOffset, replaced, nil
	}

	insertRec := encodeNodePtrRecord(sepKey, rightPage)
	idx, _ := findRecordIndex(records, sepKey, t.Compare)
	records = insertRecord(records, idx, insertRec)
	if len(records) <= t.maxRecords() {
		prev := page.PageGetPrev(pageBytes)
		next := page.PageGetNext(pageBytes)
		if !rebuildIndexPage(pageBytes, pageNo, level, prev, next, records) {
			_ = h.commit(false)
			return false, nil, fil.NullPageOffset, replaced, errors.New("btr: internal rebuild failed")
		}
		if err := h.commit(true); err != nil {
			return false, nil, fil.NullPageOffset, replaced, err
		}
		return false, nil, fil.NullPageOffset, replaced, nil
	}

	mid := len(records) / 2
	leftRecords := records[:mid]
	rightRecords := records[mid:]
	rightPage, err = t.allocPage(level)
	if err != nil {
		_ = h.commit(false)
		return false, nil, fil.NullPageOffset, replaced, err
	}
	sepKey = recordKeyOrEmpty(rightRecords)
	prev := page.PageGetPrev(pageBytes)
	next := page.PageGetNext(pageBytes)
	if !rebuildIndexPage(pageBytes, pageNo, level, prev, next, leftRecords) {
		_ = h.commit(false)
		return false, nil, fil.NullPageOffset, replaced, errors.New("btr: internal split left rebuild failed")
	}
	if err := h.commit(true); err != nil {
		return false, nil, fil.NullPageOffset, replaced, err
	}

	rh, err := t.fetchPage(rightPage)
	if err != nil {
		return false, nil, fil.NullPageOffset, replaced, err
	}
	if !rebuildIndexPage(rh.data, rightPage, level, fil.NullPageOffset, fil.NullPageOffset, rightRecords) {
		_ = rh.commit(false)
		return false, nil, fil.NullPageOffset, replaced, errors.New("btr: internal split right rebuild failed")
	}
	if err := rh.commit(true); err != nil {
		return false, nil, fil.NullPageOffset, replaced, err
	}
	return true, sepKey, rightPage, replaced, nil
}

func isNullPageNo(pageNo uint32) bool {
	return pageNo == fil.NullPageOffset
}
