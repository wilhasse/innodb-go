package btr

import (
	"bytes"
	"sort"
	"testing"

	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/ut"
)

func TestSplitLeafBytes(t *testing.T) {
	src := make([]byte, ut.UNIV_PAGE_SIZE)
	records := [][]byte{
		{0x00, 0x01, 0x01, 0x01, 0x01},
		{0x00, 0x02, 0x02, 0x02, 0x02},
		{0x00, 0x03, 0x03, 0x03, 0x03},
		{0x00, 0x04, 0x04, 0x04, 0x04},
	}
	for _, recBytes := range records {
		if _, ok := page.InsertRecordBytes(src, recBytes); !ok {
			t.Fatalf("insert failed")
		}
	}

	left, right := SplitLeafBytes(src)
	leftRecs := extractRecords(left)
	rightRecs := extractRecords(right)

	if len(leftRecs) != 2 || len(rightRecs) != 2 {
		t.Fatalf("left=%d right=%d", len(leftRecs), len(rightRecs))
	}
	if !bytes.Equal(leftRecs[0], records[0]) || !bytes.Equal(leftRecs[1], records[1]) {
		t.Fatalf("left=%v", leftRecs)
	}
	if !bytes.Equal(rightRecs[0], records[2]) || !bytes.Equal(rightRecs[1], records[3]) {
		t.Fatalf("right=%v", rightRecs)
	}
}

func extractRecords(pageBytes []byte) [][]byte {
	nSlots := int(page.HeaderGetField(pageBytes, page.PageNDirSlots))
	offsets := make([]int, 0, nSlots)
	for i := 0; i < nSlots; i++ {
		off := int(page.DirSlotGetRecOffset(pageBytes, i))
		if off != 0 {
			offsets = append(offsets, off)
		}
	}
	sort.Ints(offsets)
	heapTop := int(page.HeaderGetField(pageBytes, page.PageHeapTop))
	if heapTop == 0 {
		heapTop = int(page.PageDataOffset)
	}
	records := make([][]byte, 0, len(offsets))
	for i, off := range offsets {
		next := heapTop
		if i+1 < len(offsets) {
			next = offsets[i+1]
		}
		if next <= off {
			continue
		}
		recBytes := make([]byte, next-off)
		copy(recBytes, pageBytes[off:next])
		records = append(records, recBytes)
	}
	return records
}
