package page

import (
	"bytes"
	"testing"

	"github.com/wilhasse/innodb-go/ut"
)

func TestReorganizeReclaimsGarbage(t *testing.T) {
	page := make([]byte, ut.UNIV_PAGE_SIZE)
	rec1 := []byte{0x00, 0x00, 0x00, 0x00, 0x00}
	rec2 := []byte{0x10, 0x20, 0x30, 0x40, 0x50}

	off1, ok := InsertRecordBytes(page, rec1)
	if !ok {
		t.Fatalf("insert rec1 failed")
	}
	_, ok = InsertRecordBytes(page, rec2)
	if !ok {
		t.Fatalf("insert rec2 failed")
	}

	if !DeleteMarkRecord(page, off1, uint16(len(rec1))) {
		t.Fatalf("delete rec1 failed")
	}
	if GarbageBytes(page) == 0 {
		t.Fatalf("expected garbage")
	}

	kept := Reorganize(page)
	if kept != 1 {
		t.Fatalf("kept=%d", kept)
	}
	if GarbageBytes(page) != 0 {
		t.Fatalf("garbage=%d", GarbageBytes(page))
	}
	if FreeListHead(page) != 0 {
		t.Fatalf("free head=%d", FreeListHead(page))
	}
	if HeaderGetField(page, PageNRecs) != 1 {
		t.Fatalf("n_recs=%d", HeaderGetField(page, PageNRecs))
	}
	if HeaderGetField(page, PageNDirSlots) != 1 {
		t.Fatalf("n_slots=%d", HeaderGetField(page, PageNDirSlots))
	}
	if got := page[int(PageDataOffset) : int(PageDataOffset)+len(rec2)]; !bytes.Equal(got, rec2) {
		t.Fatalf("rec2=%v", got)
	}
	if DirSlotGetRecOffset(page, 0) != uint16(PageDataOffset) {
		t.Fatalf("slot_off=%d", DirSlotGetRecOffset(page, 0))
	}
}
