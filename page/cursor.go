package page

import "bytes"

// Record represents a simple key/value record on a page.
type Record struct {
	Key   []byte
	Value []byte
}

// Page holds ordered records.
type Page struct {
	SpaceID  uint32
	PageNo   uint32
	PageType uint16
	PrevPage uint32
	NextPage uint32
	Records []Record
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
	c.Index = 0
	return true
}

// Last positions the cursor on the last record.
func (c *Cursor) Last() bool {
	if c == nil || c.Page == nil || len(c.Page.Records) == 0 {
		return false
	}
	c.Index = len(c.Page.Records) - 1
	return true
}

// Next advances the cursor to the next record.
func (c *Cursor) Next() bool {
	if c == nil || c.Page == nil {
		return false
	}
	if c.Index+1 >= len(c.Page.Records) {
		return false
	}
	c.Index++
	return true
}

// Prev moves the cursor to the previous record.
func (c *Cursor) Prev() bool {
	if c == nil || c.Page == nil {
		return false
	}
	if c.Index-1 < 0 {
		return false
	}
	c.Index--
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
		if bytes.Compare(records[mid].Key, key) < 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	c.Index = low
	if c.Index < len(records) && bytes.Equal(records[c.Index].Key, key) {
		return true
	}
	return false
}

// Insert inserts a record at the current cursor position.
func (c *Cursor) Insert(rec Record) {
	if c == nil || c.Page == nil {
		return
	}
	idx := c.Index
	if idx < 0 || idx > len(c.Page.Records) {
		idx = len(c.Page.Records)
	}
	c.Page.Records = append(c.Page.Records, Record{})
	copy(c.Page.Records[idx+1:], c.Page.Records[idx:])
	c.Page.Records[idx] = rec
	c.Index = idx
}

// Delete removes the current record.
func (c *Cursor) Delete() {
	if c == nil || c.Page == nil {
		return
	}
	if c.Index < 0 || c.Index >= len(c.Page.Records) {
		return
	}
	copy(c.Page.Records[c.Index:], c.Page.Records[c.Index+1:])
	c.Page.Records = c.Page.Records[:len(c.Page.Records)-1]
	if c.Index >= len(c.Page.Records) {
		c.Index = len(c.Page.Records) - 1
	}
}
