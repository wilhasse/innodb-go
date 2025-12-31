package btr

import "github.com/wilhasse/innodb-go/ut"

// Cursor-related constants from btr0cur.c/h.
const (
	BtrCurPageReorganizeLimit = ut.UNIV_PAGE_SIZE / 32
	BtrCurPageCompressLimit   = ut.UNIV_PAGE_SIZE / 2

	BtrBlobHdrPartLen    = 0
	BtrBlobHdrNextPageNo = 4
	BtrBlobHdrSize       = 8

	BtrPathArraySlots = 250

	BtrCurRetryDeleteNTimes = 100
	BtrCurRetrySleepTime    = 50000

	BtrExternFieldRefSize  = 20
	BtrExternSpaceID       = 0
	BtrExternPageNo        = 4
	BtrExternOffset        = 8
	BtrExternLen           = 12
	BtrExternOwnerFlag     = 128
	BtrExternInheritedFlag = 64
)

// FieldRefZero mirrors the zeroed BLOB field reference.
var FieldRefZero [BtrExternFieldRefSize]byte

// CurMethod captures the search method used by the cursor.
type CurMethod int

const (
	CurHash CurMethod = iota + 1
	CurHashFail
	CurBinary
	CurInsertToIbuf
)

// SearchMode controls how the cursor positions relative to the key.
type SearchMode int

const (
	SearchLE SearchMode = iota
	SearchGE
)

// PathSlot stores search path info for range estimates.
type PathSlot struct {
	NthRec ut.Ulint
	NRecs  ut.Ulint
}

// Cur mirrors the btr_cur_t structure in a simplified form.
type Cur struct {
	Tree       *Tree
	Cursor     *Cursor
	Flag       CurMethod
	TreeHeight ut.Ulint
	UpMatch    ut.Ulint
	UpBytes    ut.Ulint
	LowMatch   ut.Ulint
	LowBytes   ut.Ulint
	NFields    ut.Ulint
	NBytes     ut.Ulint
	Fold       ut.Ulint
	Path       []PathSlot
}

// CurNNonSea counts cursor searches executed without adaptive hash.
var CurNNonSea ut.Ulint

// CurNSea counts cursor searches satisfied via adaptive hash.
var CurNSea ut.Ulint

// CurNNonSeaOld stores the previous non-adaptive hash counter.
var CurNNonSeaOld ut.Ulint

// CurNSeaOld stores the previous adaptive hash counter.
var CurNSeaOld ut.Ulint

// CurVarInit resets the cursor counters.
func CurVarInit() {
	CurNNonSea = 0
	CurNSea = 0
	CurNNonSeaOld = 0
	CurNSeaOld = 0
}

// NewCur allocates a tree cursor.
func NewCur(tree *Tree) *Cur {
	return &Cur{Tree: tree}
}

// Invalidate clears the cursor position.
func (c *Cur) Invalidate() {
	if c == nil {
		return
	}
	c.Cursor = nil
	c.Flag = 0
}

// Valid reports whether the cursor points at a record.
func (c *Cur) Valid() bool {
	return c != nil && c.Cursor != nil && c.Cursor.Valid()
}

// Key returns the current key.
func (c *Cur) Key() []byte {
	if !c.Valid() {
		return nil
	}
	return c.Cursor.Key()
}

// Value returns the current value.
func (c *Cur) Value() []byte {
	if !c.Valid() {
		return nil
	}
	return c.Cursor.Value()
}

// Next advances to the next record.
func (c *Cur) Next() bool {
	if c == nil || c.Cursor == nil {
		return false
	}
	return c.Cursor.Next()
}

// Prev moves to the previous record.
func (c *Cur) Prev() bool {
	if c == nil || c.Cursor == nil {
		return false
	}
	return c.Cursor.Prev()
}

// Search positions the cursor around the key using the provided mode.
func (c *Cur) Search(key []byte, mode SearchMode) bool {
	if c == nil || c.Tree == nil {
		return false
	}
	CurNNonSea++
	c.Flag = CurBinary

	var cur *Cursor
	switch mode {
	case SearchLE:
		cur = c.Tree.Seek(key)
		if cur == nil {
			cur = c.Tree.Last()
		} else if c.Tree.compare(cur.node.keys[cur.index], key) > 0 {
			if !cur.Prev() {
				cur = nil
			}
		}
	default:
		cur = c.Tree.Seek(key)
	}

	c.Cursor = cur
	return c.Valid()
}

// OpenAtIndexSide positions the cursor at the leftmost or rightmost record.
func (c *Cur) OpenAtIndexSide(left bool) bool {
	if c == nil || c.Tree == nil {
		return false
	}
	if left {
		c.Cursor = c.Tree.First()
	} else {
		c.Cursor = c.Tree.Last()
	}
	c.Flag = CurBinary
	return c.Valid()
}

// OpenAtRandom positions the cursor at a deterministic pseudo-random record.
func (c *Cur) OpenAtRandom() bool {
	if c == nil || c.Tree == nil || c.Tree.size == 0 {
		return false
	}
	if c.Tree.size%2 == 0 {
		return c.OpenAtIndexSide(true)
	}
	return c.OpenAtIndexSide(false)
}

// Insert inserts a record and positions the cursor at it.
func (c *Cur) Insert(key, value []byte) bool {
	if c == nil || c.Tree == nil {
		return false
	}
	replaced := c.Tree.Insert(key, value)
	c.Cursor = c.Tree.Seek(key)
	c.Flag = CurBinary
	return replaced
}

// Update replaces the value at the current cursor position.
func (c *Cur) Update(value []byte) bool {
	if !c.Valid() {
		return false
	}
	key := c.Cursor.node.keys[c.Cursor.index]
	c.Tree.Insert(key, value)
	c.Cursor = c.Tree.Seek(key)
	return true
}

// Delete removes the current record and advances to the next record.
func (c *Cur) Delete() bool {
	if !c.Valid() {
		return false
	}
	key := cloneBytes(c.Cursor.node.keys[c.Cursor.index])
	nextKey := []byte(nil)
	next := *c.Cursor
	if next.Next() {
		nextKey = cloneBytes(next.node.keys[next.index])
	}
	if !c.Tree.Delete(key) {
		return false
	}
	if nextKey != nil {
		c.Cursor = c.Tree.Seek(nextKey)
	} else {
		c.Cursor = nil
	}
	return true
}
