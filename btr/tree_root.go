package btr

import (
	"github.com/wilhasse/innodb-go/dict"
	"github.com/wilhasse/innodb-go/page"
)

// Create allocates a new B-tree root page and updates the index metadata.
func Create(index *dict.Index) *page.Page {
	if index == nil {
		return nil
	}
	root := PageAlloc(index)
	if root == nil {
		return nil
	}
	index.RootPage = root.PageNo
	index.TreeLevel = 0
	return root
}

// RootBlockGet returns the root page for the index.
func RootBlockGet(index *dict.Index) *page.Page {
	if index == nil {
		return nil
	}
	return page.GetPage(index.SpaceID, index.RootPage)
}

// RootGet returns the root page for the index.
func RootGet(index *dict.Index) *page.Page {
	return RootBlockGet(index)
}

// FreeRoot removes the root page and clears index metadata.
func FreeRoot(index *dict.Index) {
	if index == nil {
		return
	}
	if page.GetPage(index.SpaceID, index.RootPage) != nil {
		PageFreeLow(index.SpaceID, index.RootPage)
	}
	index.RootPage = 0
	index.TreeLevel = 0
}

// FreeButNotRoot frees all pages for the index space except the root page.
func FreeButNotRoot(index *dict.Index) {
	if index == nil {
		return
	}
	rootPageNo := index.RootPage
	pages := page.PageRegistry.Pages(index.SpaceID)
	for _, p := range pages {
		if p == nil || p.PageNo == rootPageNo {
			continue
		}
		PageFreeLow(p.SpaceID, p.PageNo)
	}
}
