package page

import (
	"bytes"
	"sort"
)

const (
	PageTypeAllocated = 0
	PageTypeIndex     = 17855
)

// NewPage allocates and initializes a page.
func NewPage(spaceID, pageNo uint32, pageType uint16) *Page {
	p := &Page{}
	p.Init(spaceID, pageNo, pageType)
	return p
}

// Init resets the page header and clears records.
func (p *Page) Init(spaceID, pageNo uint32, pageType uint16) {
	if p == nil {
		return
	}
	p.SpaceID = spaceID
	p.PageNo = pageNo
	p.PageType = pageType
	p.PrevPage = 0
	p.NextPage = 0
	p.Records = nil
}

// RecordCount returns the number of records on the page.
func (p *Page) RecordCount() int {
	if p == nil {
		return 0
	}
	return len(p.Records)
}

// IsEmpty reports whether the page has no records.
func (p *Page) IsEmpty() bool {
	return p.RecordCount() == 0
}

// InsertRecord inserts a record in key order.
func (p *Page) InsertRecord(rec Record) {
	if p == nil {
		return
	}
	idx := sort.Search(len(p.Records), func(i int) bool {
		return bytes.Compare(p.Records[i].Key, rec.Key) >= 0
	})
	p.Records = append(p.Records, Record{})
	copy(p.Records[idx+1:], p.Records[idx:])
	p.Records[idx] = rec
}

// DeleteRecord removes the record with the given key.
func (p *Page) DeleteRecord(key []byte) bool {
	if p == nil {
		return false
	}
	idx := sort.Search(len(p.Records), func(i int) bool {
		return bytes.Compare(p.Records[i].Key, key) >= 0
	})
	if idx < len(p.Records) && bytes.Equal(p.Records[idx].Key, key) {
		copy(p.Records[idx:], p.Records[idx+1:])
		p.Records = p.Records[:len(p.Records)-1]
		return true
	}
	return false
}

// FindRecord returns the record with the given key.
func (p *Page) FindRecord(key []byte) *Record {
	if p == nil {
		return nil
	}
	idx := sort.Search(len(p.Records), func(i int) bool {
		return bytes.Compare(p.Records[i].Key, key) >= 0
	})
	if idx < len(p.Records) && bytes.Equal(p.Records[idx].Key, key) {
		return &p.Records[idx]
	}
	return nil
}
