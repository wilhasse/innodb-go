package page

import "github.com/wilhasse/innodb-go/mach"

const freeListNextOffset = 3

// FreeListHead returns the offset of the first free record.
func FreeListHead(page []byte) uint16 {
	return HeaderGetField(page, PageFree)
}

// SetFreeListHead updates the free list head offset.
func SetFreeListHead(page []byte, head uint16) {
	HeaderSetField(page, PageFree, head)
}

// GarbageBytes returns the number of bytes in deleted records.
func GarbageBytes(page []byte) uint16 {
	return HeaderGetField(page, PageGarbage)
}

// SetGarbageBytes updates the number of bytes in deleted records.
func SetGarbageBytes(page []byte, bytes uint16) {
	HeaderSetField(page, PageGarbage, bytes)
}

// FreeListPush adds a record to the free list and tracks garbage bytes.
func FreeListPush(page []byte, recOff, recLen uint16) {
	if page == nil || recOff == 0 {
		return
	}
	head := FreeListHead(page)
	setFreeListNext(page, recOff, head)
	SetFreeListHead(page, recOff)
	SetGarbageBytes(page, GarbageBytes(page)+recLen)
}

// FreeListPop removes the head record from the free list.
func FreeListPop(page []byte, recLen uint16) uint16 {
	if page == nil {
		return 0
	}
	head := FreeListHead(page)
	if head == 0 {
		return 0
	}
	next := freeListNext(page, head)
	SetFreeListHead(page, next)
	garbage := GarbageBytes(page)
	if recLen >= garbage {
		SetGarbageBytes(page, 0)
	} else {
		SetGarbageBytes(page, garbage-recLen)
	}
	return head
}

func freeListNext(page []byte, recOff uint16) uint16 {
	offs := int(recOff) + freeListNextOffset
	if offs+2 > len(page) {
		return 0
	}
	return uint16(mach.ReadFrom2(page[offs:]))
}

func setFreeListNext(page []byte, recOff, next uint16) {
	offs := int(recOff) + freeListNextOffset
	if offs+2 > len(page) {
		return
	}
	mach.WriteTo2(page[offs:], uint32(next))
}
