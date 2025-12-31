package btr

import (
	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/rem"
)

// PageMaxRecords is a coarse cap used by split-fit helpers.
const PageMaxRecords = 8

// PageInsertFits reports whether a page has room for another user record.
func PageInsertFits(p *page.Page) bool {
	if p == nil {
		return false
	}
	return p.RecordCount() < PageMaxRecords
}

// PageGetSplitRecToLeft returns a split record biased to the left half.
func PageGetSplitRecToLeft(p *page.Page) *page.Record {
	recs := userRecords(p)
	if len(recs) == 0 {
		return nil
	}
	mid := (len(recs) - 1) / 2
	return recs[mid]
}

// PageGetSplitRecToRight returns a split record biased to the right half.
func PageGetSplitRecToRight(p *page.Page) *page.Record {
	recs := userRecords(p)
	if len(recs) == 0 {
		return nil
	}
	mid := len(recs) / 2
	return recs[mid]
}

// PageGetSureSplitRec returns a safe split record for a page.
func PageGetSureSplitRec(p *page.Page) *page.Record {
	if rec := PageGetSplitRecToLeft(p); rec != nil {
		return rec
	}
	return PageGetSplitRecToRight(p)
}

// PageSplitAndInsert inserts and reports whether a split was required.
func PageSplitAndInsert(t *Tree, key, value []byte) bool {
	if t == nil {
		return false
	}
	splitNeeded := false
	if t.root != nil {
		if leaf := t.findLeaf(key); leaf != nil {
			splitNeeded = len(leaf.keys) >= t.maxKeys()
		}
	}
	t.Insert(key, value)
	return splitNeeded
}

// RootRaiseAndInsert inserts and reports if the tree height increased.
func RootRaiseAndInsert(t *Tree, key, value []byte) bool {
	if t == nil {
		return false
	}
	before := treeHeight(t)
	t.Insert(key, value)
	return treeHeight(t) > before
}

func userRecords(p *page.Page) []*page.Record {
	if p == nil {
		return nil
	}
	recs := make([]*page.Record, 0)
	for i := range p.Records {
		rec := &p.Records[i]
		if rec.Type == rem.RecordUser || rec.Type == rem.RecordNodePointer {
			recs = append(recs, rec)
		}
	}
	return recs
}
