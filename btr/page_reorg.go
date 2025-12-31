package btr

import "github.com/wilhasse/innodb-go/page"

// PageReorganizeLow removes delete-marked records from the tree.
func PageReorganizeLow(t *Tree) int {
	if t == nil || t.deleted == nil {
		return 0
	}
	keys := make([]string, 0, len(t.deleted))
	for key := range t.deleted {
		keys = append(keys, key)
	}
	removed := 0
	for _, key := range keys {
		if t.Delete([]byte(key)) {
			removed++
		}
	}
	if len(t.deleted) == 0 {
		t.deleted = nil
	}
	return removed
}

// PageReorganize is a higher-level wrapper for compaction.
func PageReorganize(t *Tree) int {
	return PageReorganizeLow(t)
}

// Compress is a placeholder for page compression.
func Compress(t *Tree) int {
	return PageReorganizeLow(t)
}

// DiscardPage discards a page from the registry (stubbed).
func DiscardPage(p *page.Page) {
	if p == nil {
		return
	}
	PageFreeLow(p.SpaceID, p.PageNo)
}

// DiscardOnlyPageOnLevel is a stubbed discard helper.
func DiscardOnlyPageOnLevel(_ *Tree, _ int) {
}

// LevelListRemove is a stubbed level list removal helper.
func LevelListRemove(_ *Tree, _ int) {
}

// SetMinRecMark returns the minimum record (stubbed mark).
func SetMinRecMark(p *page.Page) *page.Record {
	if p == nil {
		return nil
	}
	cur := p.NewCursor()
	if !cur.First() {
		return nil
	}
	return cur.Record()
}
