package btr

import (
	"sort"

	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/rec"
)

// SplitLeafBytes splits a leaf page into two pages by copying record bytes.
func SplitLeafBytes(src []byte) ([]byte, []byte) {
	if src == nil {
		return nil, nil
	}
	offsets := collectSlotOffsets(src)
	if len(offsets) == 0 {
		return make([]byte, len(src)), make([]byte, len(src))
	}
	records := collectRecords(src, offsets)
	mid := len(records) / 2

	left := make([]byte, len(src))
	right := make([]byte, len(src))
	for i, recBytes := range records {
		if i < mid {
			page.InsertRecordBytes(left, recBytes)
		} else {
			page.InsertRecordBytes(right, recBytes)
		}
	}
	return left, right
}

func collectSlotOffsets(src []byte) []uint16 {
	nSlots := int(page.HeaderGetField(src, page.PageNDirSlots))
	offsets := make([]uint16, 0, nSlots)
	for i := 0; i < nSlots; i++ {
		off := page.DirSlotGetRecOffset(src, i)
		if off != 0 {
			offsets = append(offsets, off)
		}
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	return offsets
}

func collectRecords(src []byte, offsets []uint16) [][]byte {
	if len(offsets) == 0 {
		return nil
	}
	heapTop := int(page.HeaderGetField(src, page.PageHeapTop))
	if heapTop == 0 {
		heapTop = int(page.PageDataOffset)
	}
	records := make([][]byte, 0, len(offsets))
	for i, off := range offsets {
		offInt := int(off)
		if offInt < int(page.PageDataOffset) || offInt >= heapTop {
			continue
		}
		next := heapTop
		if i+1 < len(offsets) {
			next = int(offsets[i+1])
		}
		if next <= offInt {
			continue
		}
		recBytes := make([]byte, next-offInt)
		copy(recBytes, src[offInt:next])
		if rec.HeaderInfoBits(recBytes)&rec.RecInfoDeletedFlag != 0 {
			continue
		}
		records = append(records, recBytes)
	}
	return records
}
