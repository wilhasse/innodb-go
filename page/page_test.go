package page

import (
	"bytes"
	"testing"

	"github.com/wilhasse/innodb-go/ut"
)

func TestPageByteCycle(t *testing.T) {
	page := make([]byte, ut.UNIV_PAGE_SIZE)
	r1 := []byte{0x00, 0xaa, 0xaa, 0xaa, 0xaa}
	r2 := []byte{0x00, 0xbb, 0xbb, 0xbb, 0xbb}
	r3 := []byte{0x00, 0xcc, 0xcc, 0xcc, 0xcc}

	_, ok := InsertRecordBytes(page, r1)
	if !ok {
		t.Fatalf("insert r1 failed")
	}
	off2, ok := InsertRecordBytes(page, r2)
	if !ok {
		t.Fatalf("insert r2 failed")
	}
	_, ok = InsertRecordBytes(page, r3)
	if !ok {
		t.Fatalf("insert r3 failed")
	}

	if !DeleteMarkRecord(page, off2, uint16(len(r2))) {
		t.Fatalf("delete r2 failed")
	}
	if FreeListHead(page) != off2 {
		t.Fatalf("free head=%d", FreeListHead(page))
	}
	if GarbageBytes(page) == 0 {
		t.Fatalf("expected garbage")
	}

	kept := Reorganize(page)
	if kept != 2 {
		t.Fatalf("kept=%d", kept)
	}
	if HeaderGetField(page, PageNRecs) != 2 {
		t.Fatalf("n_recs=%d", HeaderGetField(page, PageNRecs))
	}
	if HeaderGetField(page, PageNDirSlots) != 2 {
		t.Fatalf("n_slots=%d", HeaderGetField(page, PageNDirSlots))
	}
	if FreeListHead(page) != 0 || GarbageBytes(page) != 0 {
		t.Fatalf("expected free list cleared")
	}

	start := int(PageDataOffset)
	if got := page[start : start+len(r1)]; !bytes.Equal(got, r1) {
		t.Fatalf("r1=%v", got)
	}
	start += len(r1)
	if got := page[start : start+len(r3)]; !bytes.Equal(got, r3) {
		t.Fatalf("r3=%v", got)
	}
}
