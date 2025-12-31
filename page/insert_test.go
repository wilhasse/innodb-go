package page

import (
	"bytes"
	"testing"

	"github.com/wilhasse/innodb-go/ut"
)

func TestInsertRecordBytes(t *testing.T) {
	page := make([]byte, ut.UNIV_PAGE_SIZE)
	rec := []byte{0x01, 0x02, 0x03}

	off, ok := InsertRecordBytes(page, rec)
	if !ok {
		t.Fatalf("insert failed")
	}
	if off != uint16(PageDataOffset) {
		t.Fatalf("off=%d", off)
	}
	if got := page[int(off) : int(off)+len(rec)]; !bytes.Equal(got, rec) {
		t.Fatalf("rec=%v", got)
	}
	if got := HeaderGetField(page, PageHeapTop); got != off+uint16(len(rec)) {
		t.Fatalf("heap_top=%d", got)
	}
	if got := HeaderGetField(page, PageNRecs); got != 1 {
		t.Fatalf("n_recs=%d", got)
	}
	if got := HeaderGetField(page, PageNDirSlots); got != 1 {
		t.Fatalf("n_slots=%d", got)
	}
	if got := DirSlotGetRecOffset(page, 0); got != off {
		t.Fatalf("slot_off=%d", got)
	}
}
