package btr

import "github.com/wilhasse/innodb-go/page"

// DeleteRecordBytes marks a record deleted and reorganizes if needed.
func DeleteRecordBytes(pageBytes []byte, keyRec []byte, nFields int) bool {
	if pageBytes == nil {
		return false
	}
	off, exact := SearchRecordBytes(pageBytes, keyRec, nFields)
	if !exact || off == 0 {
		return false
	}
	recLen := recordLength(pageBytes, off)
	if recLen == 0 {
		return false
	}
	if !page.DeleteMarkRecord(pageBytes, off, uint16(recLen)) {
		return false
	}
	if int(page.GarbageBytes(pageBytes)) > BtrCurPageReorganizeLimit {
		page.Reorganize(pageBytes)
	}
	return true
}

func recordLength(pageBytes []byte, off uint16) int {
	offsets := collectSlotOffsets(pageBytes)
	heapTop := int(page.HeaderGetField(pageBytes, page.PageHeapTop))
	if heapTop == 0 {
		heapTop = int(page.PageDataOffset)
	}
	for i, cur := range offsets {
		if cur != off {
			continue
		}
		next := heapTop
		if i+1 < len(offsets) {
			next = int(offsets[i+1])
		}
		if next <= int(off) {
			return 0
		}
		return next - int(off)
	}
	return 0
}
