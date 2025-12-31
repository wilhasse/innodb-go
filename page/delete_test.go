package page

import (
	"testing"

	"github.com/wilhasse/innodb-go/rec"
	"github.com/wilhasse/innodb-go/ut"
)

func TestDeleteMarkRecord(t *testing.T) {
	page := make([]byte, ut.UNIV_PAGE_SIZE)
	recBytes := make([]byte, rec.RecNNewExtraBytes)

	off, ok := InsertRecordBytes(page, recBytes)
	if !ok {
		t.Fatalf("insert failed")
	}
	if HeaderGetField(page, PageNRecs) != 1 {
		t.Fatalf("n_recs not updated")
	}

	if !DeleteMarkRecord(page, off, uint16(len(recBytes))) {
		t.Fatalf("delete failed")
	}
	if rec.HeaderInfoBits(page[off:])&rec.RecInfoDeletedFlag == 0 {
		t.Fatalf("deleted flag not set")
	}
	if FreeListHead(page) != off {
		t.Fatalf("free list head=%d", FreeListHead(page))
	}
	if GarbageBytes(page) != uint16(len(recBytes)) {
		t.Fatalf("garbage=%d", GarbageBytes(page))
	}
	if HeaderGetField(page, PageNRecs) != 0 {
		t.Fatalf("n_recs=%d", HeaderGetField(page, PageNRecs))
	}
}
