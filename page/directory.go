package page

import (
	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/mach"
	"github.com/wilhasse/innodb-go/rec"
)

const (
	PageDir         = fil.PageDataEnd
	PageDirSlotSize = 2
)

// DirSlotOffset returns the byte offset of the nth directory slot.
func DirSlotOffset(page []byte, slot int) int {
	if slot < 0 {
		return -1
	}
	offs := len(page) - int(PageDir) - (slot+1)*PageDirSlotSize
	if offs < 0 || offs+PageDirSlotSize > len(page) {
		return -1
	}
	return offs
}

// DirSlotGetRecOffset returns the record offset stored in a directory slot.
func DirSlotGetRecOffset(page []byte, slot int) uint16 {
	offs := DirSlotOffset(page, slot)
	if offs < 0 {
		return 0
	}
	return uint16(mach.ReadFrom2(page[offs:]))
}

// DirSlotSetRecOffset writes the record offset into a directory slot.
func DirSlotSetRecOffset(page []byte, slot int, recOff uint16) {
	offs := DirSlotOffset(page, slot)
	if offs < 0 {
		return
	}
	mach.WriteTo2(page[offs:], uint32(recOff))
}

// DirSlotGetNOwned reads the n_owned value from the record pointed by slot.
func DirSlotGetNOwned(page []byte, slot int) byte {
	recOff := DirSlotGetRecOffset(page, slot)
	if recOff == 0 {
		return 0
	}
	if int(recOff)+1 > len(page) {
		return 0
	}
	return rec.HeaderNOwned(page[recOff:])
}

// DirSlotSetNOwned updates the n_owned value on the record pointed by slot.
func DirSlotSetNOwned(page []byte, slot int, nOwned byte) {
	recOff := DirSlotGetRecOffset(page, slot)
	if recOff == 0 {
		return
	}
	if int(recOff)+1 > len(page) {
		return
	}
	rec.HeaderSetNOwned(page[recOff:], nOwned)
}
