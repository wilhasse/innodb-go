package fsp

import (
	"errors"
	"sync"

	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/ut"
)

type extent struct {
	bitmap []byte
	used   uint32
}

type spaceAlloc struct {
	extents     map[uint32]*extent
	extentCount uint32
}

var (
	allocMu sync.Mutex
	allocs  = map[uint32]*spaceAlloc{}
)

// AllocPage returns a page number from the free list or grows the space.
func AllocPage(spaceID uint32) uint32 {
	allocMu.Lock()
	defer allocMu.Unlock()

	alloc := ensureAlloc(spaceID)
	for extentIdx := uint32(0); extentIdx < alloc.extentCount; extentIdx++ {
		ext := alloc.extents[extentIdx]
		if ext == nil {
			ext = newExtent()
			alloc.extents[extentIdx] = ext
		}
		if ext.used >= uint32(ExtentSize) {
			continue
		}
		pageOff, ok := extentNextFree(ext)
		if !ok {
			continue
		}
		extentMark(ext, pageOff, true)
		pageNo := extentIdx*uint32(ExtentSize) + pageOff
		if !ensureSpaceSize(spaceID, pageNo+1) {
			return fil.NullPageOffset
		}
		if spaceID == 0 {
			_ = persistExtentMap(spaceID, alloc)
		}
		return pageNo
	}

	extentIdx := alloc.extentCount
	alloc.extentCount++
	ext := newExtent()
	alloc.extents[extentIdx] = ext
	extentMark(ext, 0, true)
	pageNo := extentIdx * uint32(ExtentSize)
	if !ensureSpaceSize(spaceID, (extentIdx+1)*uint32(ExtentSize)) {
		return fil.NullPageOffset
	}
	if spaceID == 0 {
		_ = persistExtentMap(spaceID, alloc)
	}
	return pageNo
}

// FreePage adds the page to the free list.
func FreePage(spaceID, pageNo uint32) {
	allocMu.Lock()
	defer allocMu.Unlock()

	alloc := ensureAlloc(spaceID)
	extentIdx := pageNo / uint32(ExtentSize)
	pageOff := pageNo % uint32(ExtentSize)
	if extentIdx >= alloc.extentCount {
		alloc.extentCount = extentIdx + 1
	}
	ext := alloc.extents[extentIdx]
	if ext == nil {
		ext = newExtent()
		alloc.extents[extentIdx] = ext
	}
	if extentMark(ext, pageOff, false) && spaceID == 0 {
		_ = persistExtentMap(spaceID, alloc)
	}
}

func ensureAlloc(spaceID uint32) *spaceAlloc {
	alloc := allocs[spaceID]
	if alloc == nil {
		alloc = &spaceAlloc{extents: map[uint32]*extent{}}
		allocs[spaceID] = alloc
	}
	if alloc.extents == nil {
		alloc.extents = map[uint32]*extent{}
	}
	return alloc
}

func ensureSpaceSize(spaceID uint32, minPages uint32) bool {
	space := fil.SpaceGetByID(spaceID)
	if space == nil {
		return false
	}
	if uint64(minPages) <= space.Size {
		return true
	}
	if spaceID == 0 && !space.Autoextend {
		return false
	}
	inc := uint32(uint64(minPages) - space.Size)
	HeaderIncSize(spaceID, inc)
	if len(space.Nodes) > 0 {
		last := space.Nodes[len(space.Nodes)-1]
		if last != nil {
			last.Size += uint64(inc)
			if last.File != nil {
				_ = ensureFileSize(last.File, uint64(last.Size)*ut.UNIV_PAGE_SIZE)
			}
		}
	} else if space.File != nil {
		_ = ensureFileSize(space.File, uint64(space.Size)*ut.UNIV_PAGE_SIZE)
	}
	return true
}

func extentCountForPages(pages uint32) uint32 {
	if pages == 0 {
		return 0
	}
	perExtent := uint32(ExtentSize)
	return (pages + perExtent - 1) / perExtent
}

func extentMapOffset(extentIdx uint32) int {
	return HeaderOffset + ExtentMapOffset + int(extentIdx)*extentBitmapBytes
}

func maxExtentsForPage() uint32 {
	headerStart := HeaderOffset + ExtentMapOffset
	if headerStart >= nodeMetaOffset {
		return 0
	}
	return uint32((nodeMetaOffset - headerStart) / extentBitmapBytes)
}

func newExtent() *extent {
	return &extent{bitmap: make([]byte, extentBitmapBytes)}
}

func extentMark(ext *extent, pageOff uint32, used bool) bool {
	if ext == nil || pageOff >= uint32(ExtentSize) {
		return false
	}
	byteIdx := pageOff / 8
	mask := byte(1 << (pageOff % 8))
	before := ext.bitmap[byteIdx] & mask
	if used {
		if before != 0 {
			return false
		}
		ext.bitmap[byteIdx] |= mask
		ext.used++
		return true
	}
	if before == 0 {
		return false
	}
	ext.bitmap[byteIdx] &^= mask
	if ext.used > 0 {
		ext.used--
	}
	return true
}

func extentNextFree(ext *extent) (uint32, bool) {
	if ext == nil || ext.used >= uint32(ExtentSize) {
		return 0, false
	}
	for i := uint32(0); i < uint32(ExtentSize); i++ {
		byteIdx := i / 8
		mask := byte(1 << (i % 8))
		if ext.bitmap[byteIdx]&mask == 0 {
			return i, true
		}
	}
	return 0, false
}

func extentUsedCount(bitmap []byte) uint32 {
	var count uint32
	for i := uint32(0); i < uint32(ExtentSize); i++ {
		byteIdx := i / 8
		mask := byte(1 << (i % 8))
		if bitmap[byteIdx]&mask != 0 {
			count++
		}
	}
	return count
}

func loadAllocFromHeader(spaceID uint32, page []byte) error {
	allocMu.Lock()
	defer allocMu.Unlock()

	alloc := ensureAlloc(spaceID)
	extentCount := HeaderGetExtentCount(page)
	sizePages := GetSizeLow(page)
	if extentCount == 0 {
		extentCount = extentCountForPages(sizePages)
		if extentCount == 0 && sizePages > 0 {
			extentCount = 1
		}
	}
	if extentCount > maxExtentsForPage() {
		return errors.New("fsp: extent map exceeds header capacity")
	}
	alloc.extentCount = extentCount
	alloc.extents = map[uint32]*extent{}
	if extentCount == 0 {
		return nil
	}
	for idx := uint32(0); idx < extentCount; idx++ {
		off := extentMapOffset(idx)
		if off+extentBitmapBytes > len(page) {
			return errors.New("fsp: extent map truncated")
		}
		ext := newExtent()
		copy(ext.bitmap, page[off:off+extentBitmapBytes])
		ext.used = extentUsedCount(ext.bitmap)
		alloc.extents[idx] = ext
	}
	if HeaderGetExtentCount(page) == 0 {
		ext0 := alloc.extents[0]
		if ext0 == nil {
			ext0 = newExtent()
			alloc.extents[0] = ext0
		}
		extentMark(ext0, 0, true)
		if spaceID == 0 {
			_ = persistExtentMap(spaceID, alloc)
		}
	}
	return nil
}

func persistExtentMap(spaceID uint32, alloc *spaceAlloc) error {
	if spaceID != 0 || alloc == nil {
		return nil
	}
	space := fil.SpaceGetByID(spaceID)
	if space == nil || space.File == nil {
		return nil
	}
	if alloc.extentCount > maxExtentsForPage() {
		return errors.New("fsp: extent map exceeds header capacity")
	}
	page, err := fil.ReadPage(space.File, 0)
	if err != nil {
		page = make([]byte, ut.UNIV_PAGE_SIZE)
		if initErr := initSystemHeaderPage(page, spaceID, uint32(space.Size), space.Flags); initErr != nil {
			return initErr
		}
	}
	HeaderSetExtentCount(page, alloc.extentCount)
	for idx := uint32(0); idx < alloc.extentCount; idx++ {
		off := extentMapOffset(idx)
		if off+extentBitmapBytes > len(page) {
			return errors.New("fsp: extent map truncated")
		}
		ext := alloc.extents[idx]
		if ext == nil {
			clear(page[off : off+extentBitmapBytes])
			continue
		}
		copy(page[off:off+extentBitmapBytes], ext.bitmap)
	}
	return fil.WritePage(space.File, 0, page)
}

func ensureExtentCount(spaceID uint32, count uint32) *spaceAlloc {
	allocMu.Lock()
	defer allocMu.Unlock()
	alloc := ensureAlloc(spaceID)
	if count > alloc.extentCount {
		alloc.extentCount = count
	}
	return alloc
}
