package page

import (
	"sort"

	"github.com/wilhasse/innodb-go/rec"
)

// Reorganize compacts live records and rebuilds the directory slots.
func Reorganize(page []byte) int {
	if page == nil {
		return 0
	}
	orig := make([]byte, len(page))
	copy(orig, page)

	nSlots := HeaderGetField(orig, PageNDirSlots)
	if nSlots == 0 {
		SetFreeListHead(page, 0)
		SetGarbageBytes(page, 0)
		return 0
	}
	offsets := make([]uint16, 0, nSlots)
	for i := 0; i < int(nSlots); i++ {
		off := DirSlotGetRecOffset(orig, i)
		if off != 0 {
			offsets = append(offsets, off)
		}
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })

	heapTop := int(HeaderGetField(orig, PageHeapTop))
	if heapTop == 0 {
		heapTop = int(PageDataOffset)
	}

	type recInfo struct {
		off   uint16
		len   int
		live  bool
		bytes []byte
	}
	records := make([]recInfo, 0, len(offsets))
	for i, off := range offsets {
		offInt := int(off)
		if offInt < int(PageDataOffset) || offInt >= heapTop {
			continue
		}
		next := heapTop
		if i+1 < len(offsets) {
			next = int(offsets[i+1])
		}
		if next <= offInt {
			continue
		}
		length := next - offInt
		deleted := rec.HeaderInfoBits(orig[off:])&rec.RecInfoDeletedFlag != 0
		records = append(records, recInfo{
			off:   off,
			len:   length,
			live:  !deleted,
			bytes: orig[offInt:next],
		})
	}

	dataEnd := len(page) - int(PageDir)
	for i := int(PageDataOffset); i < dataEnd; i++ {
		page[i] = 0
	}

	newOffsets := make([]uint16, 0, len(records))
	writePos := int(PageDataOffset)
	for _, recInfo := range records {
		if !recInfo.live {
			continue
		}
		if writePos+recInfo.len > dataEnd {
			break
		}
		copy(page[writePos:writePos+recInfo.len], recInfo.bytes)
		newOffsets = append(newOffsets, uint16(writePos))
		writePos += recInfo.len
	}

	HeaderSetField(page, PageHeapTop, uint16(writePos))
	HeaderSetField(page, PageNRecs, uint16(len(newOffsets)))
	HeaderSetField(page, PageNDirSlots, uint16(len(newOffsets)))
	nHeap := HeaderGetField(orig, PageNHeap)
	HeaderSetField(page, PageNHeap, (nHeap&0x8000)|uint16(len(newOffsets)))
	HeaderSetField(page, PageGarbage, 0)
	HeaderSetField(page, PageFree, 0)

	for i := 0; i < int(nSlots); i++ {
		if i < len(newOffsets) {
			DirSlotSetRecOffset(page, i, newOffsets[i])
		} else {
			DirSlotSetRecOffset(page, i, 0)
		}
	}
	return len(newOffsets)
}
