package fsp

import (
	"sync"

	"github.com/wilhasse/innodb-go/fil"
)

type spaceAlloc struct {
	free []uint32
}

var (
	allocMu sync.Mutex
	allocs  = map[uint32]*spaceAlloc{}
)

// AllocPage returns a page number from the free list or grows the space.
func AllocPage(spaceID uint32) uint32 {
	allocMu.Lock()
	defer allocMu.Unlock()

	alloc := allocs[spaceID]
	if alloc == nil {
		alloc = &spaceAlloc{}
		allocs[spaceID] = alloc
	}
	if n := len(alloc.free); n > 0 {
		pageNo := alloc.free[n-1]
		alloc.free = alloc.free[:n-1]
		return pageNo
	}

	space := fil.SpaceGetByID(spaceID)
	if space == nil {
		return fil.NullPageOffset
	}
	pageNo := uint32(space.Size)
	HeaderIncSize(spaceID, 1)
	return pageNo
}

// FreePage adds the page to the free list.
func FreePage(spaceID, pageNo uint32) {
	allocMu.Lock()
	defer allocMu.Unlock()

	alloc := allocs[spaceID]
	if alloc == nil {
		alloc = &spaceAlloc{}
		allocs[spaceID] = alloc
	}
	alloc.free = append(alloc.free, pageNo)
}
