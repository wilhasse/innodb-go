package fsp

import (
	"encoding/binary"

	"github.com/wilhasse/innodb-go/fil"
)

var currentFreeLimit uint32

// Init initializes the file space subsystem.
func Init() {
	currentFreeLimit = 0
}

// HeaderGetFreeLimit returns the current free limit of space 0.
func HeaderGetFreeLimit() uint32 {
	return currentFreeLimit
}

// HeaderGetTablespaceSize returns the cached size of space 0.
func HeaderGetTablespaceSize() uint32 {
	return uint32(fil.SpaceGetSize(0))
}

// GetSizeLow reads the space size from the header page.
func GetSizeLow(page []byte) uint32 {
	return readUint32(page, HeaderOffset+SizeOffset)
}

// HeaderGetSpaceID reads the space id from the header page.
func HeaderGetSpaceID(page []byte) uint32 {
	return readUint32(page, HeaderOffset+SpaceIDOffset)
}

// HeaderGetFlags reads the space flags from the header page.
func HeaderGetFlags(page []byte) uint32 {
	return readUint32(page, HeaderOffset+SpaceFlagsOffset)
}

// HeaderGetZipSize returns the stored compressed page size, if any.
func HeaderGetZipSize(page []byte) uint32 {
	flags := HeaderGetFlags(page)
	if flags == 0 {
		return 0
	}
	return flags
}

// HeaderInitFields writes space id and flags to the header page.
func HeaderInitFields(page []byte, spaceID uint32, flags uint32) {
	writeUint32(page, HeaderOffset+SpaceIDOffset, spaceID)
	writeUint32(page, HeaderOffset+SpaceFlagsOffset, flags)
}

// HeaderInit initializes the basic header fields on a page.
func HeaderInit(page []byte, spaceID uint32, size uint32, flags uint32) {
	HeaderInitFields(page, spaceID, flags)
	writeUint32(page, HeaderOffset+SizeOffset, size)
	writeUint32(page, HeaderOffset+FreeLimitOffset, 0)
	currentFreeLimit = size
}

// HeaderIncSize increments the cached size for a space.
func HeaderIncSize(spaceID uint32, sizeInc uint32) {
	space := fil.SpaceGetByID(spaceID)
	if space == nil {
		return
	}
	space.Size += uint64(sizeInc)
	if spaceID == 0 {
		currentFreeLimit += sizeInc
	}
}

func readUint32(page []byte, offset int) uint32 {
	if len(page) < offset+4 {
		return 0
	}
	return binary.BigEndian.Uint32(page[offset : offset+4])
}

func writeUint32(page []byte, offset int, val uint32) {
	if len(page) < offset+4 {
		return
	}
	binary.BigEndian.PutUint32(page[offset:offset+4], val)
}
