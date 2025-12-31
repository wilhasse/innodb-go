package page

import (
	"bytes"

	"github.com/wilhasse/innodb-go/rem"
)

// Record represents a simple key/value record on a page.
type Record struct {
	Type   rem.RecordType
	HeapNo uint16
	Key    []byte
	Value  []byte
}

// Page holds ordered records.
type Page struct {
	SpaceID      uint32
	PageNo       uint32
	PageType     uint16
	PrevPage     uint32
	NextPage     uint32
	ParentPageNo uint32
	NextHeapNo   uint16
	Records      []Record
}

// Cursor points to a record within a page.
type Cursor struct {
	Page  *Page
	Index int
}

// NewCursor creates a cursor for the page.
func (p *Page) NewCursor() *Cursor {
	return &Cursor{Page: p, Index: -1}
}

// Position sets the cursor to an absolute index.
func (c *Cursor) Position(index int) {
	if c == nil {
		return
	}
	c.Index = index
}

// Record returns the current record.
func (c *Cursor) Record() *Record {
	if c == nil || c.Page == nil {
		return nil
	}
	if c.Index < 0 || c.Index >= len(c.Page.Records) {
		return nil
	}
	return &c.Page.Records[c.Index]
}

// First positions the cursor on the first record.
func (c *Cursor) First() bool {
	if c == nil || c.Page == nil || len(c.Page.Records) == 0 {
		return false
	}
	idx := nextUserIndex(c.Page.Records, 0)
	if idx >= len(c.Page.Records) {
		return false
	}
	c.Index = idx
	return true
}

// Last positions the cursor on the last record.
func (c *Cursor) Last() bool {
	if c == nil || c.Page == nil || len(c.Page.Records) == 0 {
		return false
	}
	idx := prevUserIndex(c.Page.Records, len(c.Page.Records)-1)
	if idx < 0 {
		return false
	}
	c.Index = idx
	return true
}

// Next advances the cursor to the next record.
func (c *Cursor) Next() bool {
	if c == nil || c.Page == nil {
		return false
	}
	start := c.Index + 1
	if start < 0 {
		start = 0
	}
	idx := nextUserIndex(c.Page.Records, start)
	if idx >= len(c.Page.Records) {
		return false
	}
	c.Index = idx
	return true
}

// Prev moves the cursor to the previous record.
func (c *Cursor) Prev() bool {
	if c == nil || c.Page == nil {
		return false
	}
	start := c.Index - 1
	if start >= len(c.Page.Records) {
		start = len(c.Page.Records) - 1
	}
	idx := prevUserIndex(c.Page.Records, start)
	if idx < 0 {
		return false
	}
	c.Index = idx
	return true
}

// Search positions the cursor on the first record >= key.
func (c *Cursor) Search(key []byte) bool {
	if c == nil || c.Page == nil {
		return false
	}
	records := c.Page.Records
	low, high := 0, len(records)
	for low < high {
		mid := (low + high) / 2
		if compareRecordToKey(records[mid], key) < 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	idx := nextUserIndex(records, low)
	if idx >= len(records) {
		return false
	}
	c.Index = idx
	return bytes.Equal(records[idx].Key, key)
}

// Insert inserts a record at the current cursor position.
func (c *Cursor) Insert(rec Record) {
	if c == nil || c.Page == nil {
		return
	}
	c.Page.InsertRecord(rec)
	if c.Search(rec.Key) {
		return
	}
	c.First()
}

// Delete removes the current record.
func (c *Cursor) Delete() {
	if c == nil || c.Page == nil {
		return
	}
	if c.Index < 0 || c.Index >= len(c.Page.Records) {
		return
	}
	if !isUserRecord(c.Page.Records[c.Index]) {
		return
	}
	c.Page.DeleteRecord(c.Page.Records[c.Index].Key)
	if c.Index >= len(c.Page.Records) {
		c.Index = len(c.Page.Records) - 1
	}
}

// SlotCursor navigates raw page directory slots in order.
type SlotCursor struct {
	Page []byte
	Slot int
}

// NewSlotCursor creates a cursor for raw page bytes.
func NewSlotCursor(page []byte) *SlotCursor {
	return &SlotCursor{Page: page, Slot: -1}
}

// Valid reports whether the cursor points to a valid slot.
func (c *SlotCursor) Valid() bool {
	if c == nil || c.Page == nil {
		return false
	}
	nSlots := int(HeaderGetField(c.Page, PageNDirSlots))
	return c.Slot >= 0 && c.Slot < nSlots
}

// First positions the cursor on the first slot.
func (c *SlotCursor) First() bool {
	if c == nil || c.Page == nil {
		return false
	}
	nSlots := int(HeaderGetField(c.Page, PageNDirSlots))
	if nSlots == 0 {
		return false
	}
	c.Slot = 0
	return true
}

// Last positions the cursor on the last slot.
func (c *SlotCursor) Last() bool {
	if c == nil || c.Page == nil {
		return false
	}
	nSlots := int(HeaderGetField(c.Page, PageNDirSlots))
	if nSlots == 0 {
		return false
	}
	c.Slot = nSlots - 1
	return true
}

// Next advances to the next slot.
func (c *SlotCursor) Next() bool {
	if c == nil || c.Page == nil {
		return false
	}
	nSlots := int(HeaderGetField(c.Page, PageNDirSlots))
	if c.Slot+1 >= nSlots {
		return false
	}
	c.Slot++
	return true
}

// Prev moves to the previous slot.
func (c *SlotCursor) Prev() bool {
	if c == nil || c.Page == nil {
		return false
	}
	if c.Slot <= 0 {
		return false
	}
	c.Slot--
	return true
}

// RecordOffset returns the record offset for the current slot.
func (c *SlotCursor) RecordOffset() uint16 {
	if !c.Valid() {
		return 0
	}
	return DirSlotGetRecOffset(c.Page, c.Slot)
}
