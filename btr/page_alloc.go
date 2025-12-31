package btr

import (
	"github.com/wilhasse/innodb-go/buf"
	"github.com/wilhasse/innodb-go/dict"
	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/fsp"
	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/ut"
)

// PageAlloc allocates a new B-tree page for the index.
func PageAlloc(index *dict.Index) *page.Page {
	if index == nil {
		return nil
	}
	pageNo := fsp.AllocPage(index.SpaceID)
	if pageNo == fil.NullPageOffset {
		return nil
	}
	p := PageCreate(index.SpaceID, pageNo)
	page.RegisterPage(p)

	if pool := buf.GetDefaultPool(); pool != nil {
		if bufPage, _, err := pool.Fetch(index.SpaceID, pageNo); err == nil {
			clear(bufPage.Data)
			pool.MarkDirty(bufPage)
			_ = pool.FlushPage(bufPage.ID)
			pool.Release(bufPage)
		}
	} else {
		_ = fil.SpaceWritePage(index.SpaceID, pageNo, make([]byte, ut.UNIV_PAGE_SIZE))
	}
	return p
}

// PageFreeLow releases a page back to the free list and registry.
func PageFreeLow(spaceID, pageNo uint32) {
	page.DeletePage(spaceID, pageNo)
	fsp.FreePage(spaceID, pageNo)
	if pool := buf.GetDefaultPool(); pool != nil {
		pool.Drop(spaceID, pageNo)
	}
}

// PageFree releases an allocated page.
func PageFree(_ *dict.Index, p *page.Page) {
	if p == nil {
		return
	}
	PageFreeLow(p.SpaceID, p.PageNo)
}

// GetSize returns the number of allocated pages for the index space.
func GetSize(index *dict.Index) ut.Ulint {
	if index == nil {
		return 0
	}
	return ut.Ulint(page.PageRegistry.Count(index.SpaceID))
}
