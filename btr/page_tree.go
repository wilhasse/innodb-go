package btr

import (
	"bytes"
	"errors"
	"sort"

	"github.com/wilhasse/innodb-go/buf"
	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/fsp"
	"github.com/wilhasse/innodb-go/mtr"
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
	if err := t.ensureRootInitialized(); err != nil {
		return false, err
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
	if !rebuildIndexPage(h.data, t.SpaceID, newRoot, rootLevel+1, fil.NullPageOffset, fil.NullPageOffset, records) {
		_ = h.commit(false)
		return false, errors.New("btr: root rebuild failed")
	}
	t.logPageWrite(h.data)
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
		records := t.sortRecords(collectUserRecords(h.data))
		child, ok := findChildPage(records, key, t.Compare)
		_ = h.commit(false)
		if !ok {
			return nil, false, errors.New("btr: missing child page")
		}
		pageNo = child
	}
}

// Delete removes a key/value pair by key.
func (t *PageTree) Delete(key []byte) (bool, error) {
	if t == nil {
		return false, errors.New("btr: nil tree")
	}
	if t.RootPage == fil.NullPageOffset {
		return false, nil
	}
	t.ensureDefaults()
	if err := t.ensureRootInitialized(); err != nil {
		return false, err
	}

	pageNo := t.RootPage
	for {
		h, err := t.fetchPage(pageNo)
		if err != nil {
			return false, err
		}
		level := page.PageGetLevel(h.data)
		if level == 0 {
			records := collectUserRecords(h.data)
			idx, exact := findRecordIndex(records, key, t.Compare)
			if !exact {
				_ = h.commit(false)
				return false, nil
			}
			records = append(records[:idx], records[idx+1:]...)
			prev := page.PageGetPrev(h.data)
			next := page.PageGetNext(h.data)
			if !rebuildIndexPage(h.data, t.SpaceID, pageNo, level, prev, next, records) {
				_ = h.commit(false)
				return false, errors.New("btr: leaf delete rebuild failed")
			}
			t.logPageWrite(h.data)
			if err := h.commit(true); err != nil {
				return false, err
			}
			if t.size > 0 {
				t.size--
			}
			return true, nil
		}
		records := t.sortRecords(collectUserRecords(h.data))
		child, ok := findChildPage(records, key, t.Compare)
		_ = h.commit(false)
		if !ok {
			return false, errors.New("btr: missing child page")
		}
		pageNo = child
	}
}

// ForEach iterates all leaf records in key order.
func (t *PageTree) ForEach(fn func(key, value []byte) bool) error {
	if t == nil || fn == nil {
		return nil
	}
	if t.RootPage == fil.NullPageOffset {
		return nil
	}
	t.ensureDefaults()
	if err := t.ensureRootInitialized(); err != nil {
		return err
	}
	root, err := t.fetchPage(t.RootPage)
	if err != nil {
		return err
	}
	rootType := page.PageGetType(root.data)
	if rootType == fil.PageTypeAllocated {
		_ = root.commit(false)
		return nil
	}
	if rootType != fil.PageTypeIndex {
		_ = root.commit(false)
		return errors.New("btr: root not an index page")
	}
	_ = root.commit(false)
	start, err := t.leftmostLeaf()
	if err != nil {
		return err
	}
	pageNo := start
	for !isNullPageNo(pageNo) {
		h, err := t.fetchPage(pageNo)
		if err != nil {
			return err
		}
		records := t.sortRecords(collectUserRecords(h.data))
		next := page.PageGetNext(h.data)
		if next == 0 {
			_ = h.commit(false)
			return errors.New("btr: invalid next page")
		}
		for _, recBytes := range records {
			key, val, ok := decodeLeafRecord(recBytes)
			if !ok {
				continue
			}
			if !fn(key, val) {
				_ = h.commit(false)
				return nil
			}
		}
		if err := h.commit(false); err != nil {
			return err
		}
		pageNo = next
	}
	return nil
}

func (t *PageTree) leftmostLeaf() (uint32, error) {
	pageNo := t.RootPage
	for {
		h, err := t.fetchPage(pageNo)
		if err != nil {
			return 0, err
		}
		if page.PageGetType(h.data) != fil.PageTypeIndex {
			_ = h.commit(false)
			return 0, errors.New("btr: non-index page")
		}
		level := page.PageGetLevel(h.data)
		if level == 0 {
			_ = h.commit(false)
			return pageNo, nil
		}
		records := t.sortRecords(collectUserRecords(h.data))
		if len(records) == 0 {
			_ = h.commit(false)
			return 0, errors.New("btr: empty internal page")
		}
		_, child, ok := decodeNodePtrRecord(records[0])
		_ = h.commit(false)
		if !ok {
			return 0, errors.New("btr: invalid node pointer")
		}
		pageNo = child
	}
}

func (t *PageTree) splitLeafRecords(records [][]byte) ([][]byte, [][]byte, bool) {
	if t == nil || len(records) < 2 {
		return nil, nil, false
	}
	for split := 1; split < len(records); split++ {
		left := records[:split]
		right := records[split:]
		if t.canFitRecords(left) && t.canFitRecords(right) {
			return left, right, true
		}
	}
	return nil, nil, false
}

func (t *PageTree) canFitRecords(records [][]byte) bool {
	if t == nil {
		return false
	}
	if len(records) == 0 {
		return true
	}
	buf := make([]byte, ut.UNIV_PAGE_SIZE)
	return rebuildIndexPage(buf, t.SpaceID, 0, 0, fil.NullPageOffset, fil.NullPageOffset, records)
}

func (t *PageTree) logPageWrite(pageBytes []byte) {
	if t == nil || pageBytes == nil {
		return
	}
	var mini mtr.Mtr
	mtr.Start(&mini)
	mtr.MlogLogString(pageBytes, 0, ut.UNIV_PAGE_SIZE, &mini)
	mtr.Commit(&mini)
}

func (t *PageTree) ensureDefaults() {
	if t.Compare == nil {
		t.Compare = bytes.Compare
	}
	if t.MaxRecs <= 0 {
		t.MaxRecs = PageMaxRecords
	}
}

func (t *PageTree) ensureRootInitialized() error {
	if t == nil || t.RootPage == fil.NullPageOffset {
		return nil
	}
	h, err := t.fetchPage(t.RootPage)
	if err != nil {
		return err
	}
	if page.PageGetType(h.data) != fil.PageTypeIndex {
		if !initIndexPageBytes(h.data, t.SpaceID, t.RootPage, 0) {
			_ = h.commit(false)
			return errors.New("btr: root init failed")
		}
		t.logPageWrite(h.data)
		return h.commit(true)
	}
	if page.PageGetSpaceID(h.data) != t.SpaceID {
		page.PageSetSpaceID(h.data, t.SpaceID)
		page.PageSetPageNo(h.data, t.RootPage)
		t.logPageWrite(h.data)
		return h.commit(true)
	}
	return h.commit(false)
}

func (t *PageTree) maxRecords() int {
	if t.MaxRecs <= 0 {
		return PageMaxRecords
	}
	return t.MaxRecs
}

func (t *PageTree) sortRecords(records [][]byte) [][]byte {
	if t == nil || len(records) < 2 {
		return records
	}
	type recItem struct {
		rec []byte
		key []byte
		ok  bool
	}
	items := make([]recItem, 0, len(records))
	for _, recBytes := range records {
		key, ok := recordKey(recBytes)
		items = append(items, recItem{rec: recBytes, key: key, ok: ok})
	}
	sort.SliceStable(items, func(i, j int) bool {
		if !items[i].ok && !items[j].ok {
			return false
		}
		if !items[i].ok {
			return false
		}
		if !items[j].ok {
			return true
		}
		return t.Compare(items[i].key, items[j].key) < 0
	})
	for i := range items {
		records[i] = items[i].rec
	}
	return records
}

func (t *PageTree) refreshNodePtrRecords(records [][]byte) [][]byte {
	if t == nil || len(records) == 0 {
		return records
	}
	updated := make([][]byte, 0, len(records))
	for _, recBytes := range records {
		key, child, ok := decodeNodePtrRecord(recBytes)
		if !ok {
			updated = append(updated, recBytes)
			continue
		}
		minKey, err := t.pageMinKey(child)
		if err != nil || len(minKey) == 0 {
			minKey = key
		}
		updated = append(updated, encodeNodePtrRecord(minKey, child))
	}
	return t.sortRecords(updated)
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
	if !initIndexPageBytes(h.data, t.SpaceID, pageNo, level) {
		_ = h.commit(false)
		return fil.NullPageOffset, errors.New("btr: init page failed")
	}
	t.logPageWrite(h.data)
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
	level := page.PageGetLevel(h.data)
	records := t.sortRecords(collectUserRecords(h.data))
	if len(records) == 0 {
		_ = h.commit(false)
		return nil, nil
	}
	if level == 0 {
		key, ok := recordKey(records[0])
		_ = h.commit(false)
		if !ok {
			return nil, nil
		}
		return key, nil
	}
	_, child, ok := decodeNodePtrRecord(records[0])
	_ = h.commit(false)
	if !ok {
		return nil, nil
	}
	return t.pageMinKey(child)
}

func (t *PageTree) insertPage(pageNo uint32, key, value []byte) (bool, []byte, uint32, bool, error) {
	h, err := t.fetchPage(pageNo)
	if err != nil {
		return false, nil, fil.NullPageOffset, false, err
	}
	pageBytes := h.data
	level := page.PageGetLevel(pageBytes)
	if level == 0 {
		records := t.sortRecords(collectUserRecords(pageBytes))
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
			if rebuildIndexPage(pageBytes, t.SpaceID, pageNo, level, prev, next, records) {
				t.logPageWrite(pageBytes)
				if err := h.commit(true); err != nil {
					return false, nil, fil.NullPageOffset, exact, err
				}
				return false, nil, fil.NullPageOffset, exact, nil
			}
			if len(records) <= 1 {
				_ = h.commit(false)
				return false, nil, fil.NullPageOffset, exact, errors.New("btr: leaf rebuild failed")
			}
		}

		leftRecords, rightRecords, ok := t.splitLeafRecords(records)
		if !ok {
			_ = h.commit(false)
			return false, nil, fil.NullPageOffset, exact, errors.New("btr: leaf split failed")
		}
		rightPage, err := t.allocPage(0)
		if err != nil {
			_ = h.commit(false)
			return false, nil, fil.NullPageOffset, exact, err
		}
		sepKey := recordKeyOrEmpty(rightRecords)
		prev := page.PageGetPrev(pageBytes)
		next := page.PageGetNext(pageBytes)
		if !rebuildIndexPage(pageBytes, t.SpaceID, pageNo, 0, prev, rightPage, leftRecords) {
			_ = h.commit(false)
			return false, nil, fil.NullPageOffset, exact, errors.New("btr: leaf split rebuild failed")
		}
		t.logPageWrite(pageBytes)
		if err := h.commit(true); err != nil {
			return false, nil, fil.NullPageOffset, exact, err
		}

		rh, err := t.fetchPage(rightPage)
		if err != nil {
			return false, nil, fil.NullPageOffset, exact, err
		}
		if !rebuildIndexPage(rh.data, t.SpaceID, rightPage, 0, pageNo, next, rightRecords) {
			_ = rh.commit(false)
			return false, nil, fil.NullPageOffset, exact, errors.New("btr: leaf split right rebuild failed")
		}
		t.logPageWrite(rh.data)
		if err := rh.commit(true); err != nil {
			return false, nil, fil.NullPageOffset, exact, err
		}
		if !isNullPageNo(next) {
			nh, err := t.fetchPage(next)
			if err != nil {
				return false, nil, fil.NullPageOffset, exact, err
			}
			page.PageSetPrev(nh.data, rightPage)
			t.logPageWrite(nh.data)
			if err := nh.commit(true); err != nil {
				return false, nil, fil.NullPageOffset, exact, err
			}
		}
		return true, sepKey, rightPage, exact, nil
	}

	records := t.sortRecords(collectUserRecords(pageBytes))
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

	if split {
		rightKey, err := t.pageMinKey(rightPage)
		if err != nil || len(rightKey) == 0 {
			rightKey = sepKey
		}
		insertRec := encodeNodePtrRecord(rightKey, rightPage)
		idx, _ := findRecordIndex(records, rightKey, t.Compare)
		records = insertRecord(records, idx, insertRec)
	}
	records = t.refreshNodePtrRecords(records)
	if len(records) <= t.maxRecords() {
		prev := page.PageGetPrev(pageBytes)
		next := page.PageGetNext(pageBytes)
		if !rebuildIndexPage(pageBytes, t.SpaceID, pageNo, level, prev, next, records) {
			_ = h.commit(false)
			return false, nil, fil.NullPageOffset, replaced, errors.New("btr: internal rebuild failed")
		}
		t.logPageWrite(pageBytes)
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
	if !rebuildIndexPage(pageBytes, t.SpaceID, pageNo, level, prev, next, leftRecords) {
		_ = h.commit(false)
		return false, nil, fil.NullPageOffset, replaced, errors.New("btr: internal split left rebuild failed")
	}
	t.logPageWrite(pageBytes)
	if err := h.commit(true); err != nil {
		return false, nil, fil.NullPageOffset, replaced, err
	}

	rh, err := t.fetchPage(rightPage)
	if err != nil {
		return false, nil, fil.NullPageOffset, replaced, err
	}
	if !rebuildIndexPage(rh.data, t.SpaceID, rightPage, level, fil.NullPageOffset, fil.NullPageOffset, rightRecords) {
		_ = rh.commit(false)
		return false, nil, fil.NullPageOffset, replaced, errors.New("btr: internal split right rebuild failed")
	}
	t.logPageWrite(rh.data)
	if err := rh.commit(true); err != nil {
		return false, nil, fil.NullPageOffset, replaced, err
	}
	return true, sepKey, rightPage, replaced, nil
}

func isNullPageNo(pageNo uint32) bool {
	return pageNo == fil.NullPageOffset
}
