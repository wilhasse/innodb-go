package page

import (
	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/mach"
)

// Page header offsets mirror page0page.h.
const (
	PageHeaderOffset = fil.PageData
	FsegHeaderSize   = 10
	PageDataOffset   = PageHeaderOffset + 36 + 2*FsegHeaderSize

	PageNDirSlots  uint32 = 0
	PageHeapTop    uint32 = 2
	PageNHeap      uint32 = 4
	PageFree       uint32 = 6
	PageGarbage    uint32 = 8
	PageLastInsert uint32 = 10
	PageDirection  uint32 = 12
	PageNDirection uint32 = 14
	PageNRecs      uint32 = 16
	PageMaxTrxID   uint32 = 18
	PageLevel      uint32 = 26
	PageIndexID    uint32 = 28
)

// HeaderGetField reads a 2-byte page header field.
func HeaderGetField(page []byte, field uint32) uint16 {
	offs := int(PageHeaderOffset + field)
	if offs+2 > len(page) {
		return 0
	}
	return uint16(mach.ReadFrom2(page[offs:]))
}

// HeaderSetField writes a 2-byte page header field.
func HeaderSetField(page []byte, field uint32, val uint16) {
	offs := int(PageHeaderOffset + field)
	if offs+2 > len(page) {
		return
	}
	mach.WriteTo2(page[offs:], uint32(val))
}

// PageGetPageNo reads the page number from the file header.
func PageGetPageNo(page []byte) uint32 {
	offs := int(fil.PageOffset)
	if offs+4 > len(page) {
		return 0
	}
	return mach.ReadFrom4(page[offs:])
}

// PageGetSpaceID reads the space id from the file header.
func PageGetSpaceID(page []byte) uint32 {
	offs := int(fil.PageArchLogNoOrSpaceID)
	if offs+4 > len(page) {
		return 0
	}
	return mach.ReadFrom4(page[offs:])
}

// PageSetPageNo writes the page number into the file header.
func PageSetPageNo(page []byte, pageNo uint32) {
	offs := int(fil.PageOffset)
	if offs+4 > len(page) {
		return
	}
	mach.WriteTo4(page[offs:], pageNo)
}

// PageSetSpaceID writes the space id into the file header.
func PageSetSpaceID(page []byte, spaceID uint32) {
	offs := int(fil.PageArchLogNoOrSpaceID)
	if offs+4 > len(page) {
		return
	}
	mach.WriteTo4(page[offs:], spaceID)
}

// PageGetLevel returns the page level from the header.
func PageGetLevel(page []byte) uint16 {
	return HeaderGetField(page, PageLevel)
}

// PageSetLevel sets the page level in the header.
func PageSetLevel(page []byte, level uint16) {
	HeaderSetField(page, PageLevel, level)
}

// PageGetNRecs returns the number of user records on the page.
func PageGetNRecs(page []byte) uint16 {
	return HeaderGetField(page, PageNRecs)
}

// PageSetNRecs sets the number of user records on the page.
func PageSetNRecs(page []byte, nRecs uint16) {
	HeaderSetField(page, PageNRecs, nRecs)
}

// PageGetPrev returns the previous page number from the file header.
func PageGetPrev(page []byte) uint32 {
	offs := int(fil.PagePrev)
	if offs+4 > len(page) {
		return 0
	}
	return mach.ReadFrom4(page[offs:])
}

// PageSetPrev writes the previous page number into the file header.
func PageSetPrev(page []byte, prev uint32) {
	offs := int(fil.PagePrev)
	if offs+4 > len(page) {
		return
	}
	mach.WriteTo4(page[offs:], prev)
}

// PageGetNext returns the next page number from the file header.
func PageGetNext(page []byte) uint32 {
	offs := int(fil.PageNext)
	if offs+4 > len(page) {
		return 0
	}
	return mach.ReadFrom4(page[offs:])
}

// PageSetNext writes the next page number into the file header.
func PageSetNext(page []byte, next uint32) {
	offs := int(fil.PageNext)
	if offs+4 > len(page) {
		return
	}
	mach.WriteTo4(page[offs:], next)
}

// PageSetType sets the page type in the file header.
func PageSetType(page []byte, pageType uint16) {
	offs := int(fil.PageType)
	if offs+2 > len(page) {
		return
	}
	mach.WriteTo2(page[offs:], uint32(pageType))
}

// PageGetType returns the page type from the file header.
func PageGetType(page []byte) uint16 {
	offs := int(fil.PageType)
	if offs+2 > len(page) {
		return 0
	}
	return uint16(mach.ReadFrom2(page[offs:]))
}
