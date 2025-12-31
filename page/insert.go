package page

// InsertRecordBytes appends a record to the heap and updates header/dir slots.
func InsertRecordBytes(page []byte, rec []byte) (uint16, bool) {
	if len(page) == 0 || len(rec) == 0 {
		return 0, false
	}
	heapTop := HeaderGetField(page, PageHeapTop)
	if heapTop == 0 {
		heapTop = uint16(PageDataOffset)
	}
	nSlots := HeaderGetField(page, PageNDirSlots)
	slotOff := DirSlotOffset(page, int(nSlots))
	if slotOff < 0 {
		return 0, false
	}
	if int(heapTop)+len(rec) > slotOff {
		return 0, false
	}
	copy(page[int(heapTop):], rec)

	newHeapTop := heapTop + uint16(len(rec))
	HeaderSetField(page, PageHeapTop, newHeapTop)

	nHeap := HeaderGetField(page, PageNHeap)
	HeaderSetField(page, PageNHeap, (nHeap&0x8000)|((nHeap&0x7fff)+1))
	HeaderSetField(page, PageNRecs, HeaderGetField(page, PageNRecs)+1)

	DirSlotSetRecOffset(page, int(nSlots), heapTop)
	HeaderSetField(page, PageNDirSlots, nSlots+1)

	return heapTop, true
}
