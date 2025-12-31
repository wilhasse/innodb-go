package page

import (
	"testing"

	"github.com/wilhasse/innodb-go/ut"
)

func TestSlotCursorTraversal(t *testing.T) {
	page := make([]byte, ut.UNIV_PAGE_SIZE)
	off1, ok := InsertRecordBytes(page, []byte{0x01})
	if !ok {
		t.Fatalf("insert r1 failed")
	}
	off2, ok := InsertRecordBytes(page, []byte{0x02, 0x02})
	if !ok {
		t.Fatalf("insert r2 failed")
	}
	off3, ok := InsertRecordBytes(page, []byte{0x03, 0x03, 0x03})
	if !ok {
		t.Fatalf("insert r3 failed")
	}

	cur := NewSlotCursor(page)
	if !cur.First() {
		t.Fatalf("first failed")
	}
	if cur.RecordOffset() != off1 {
		t.Fatalf("first off=%d", cur.RecordOffset())
	}
	if !cur.Next() || cur.RecordOffset() != off2 {
		t.Fatalf("next off=%d", cur.RecordOffset())
	}
	if !cur.Next() || cur.RecordOffset() != off3 {
		t.Fatalf("next off=%d", cur.RecordOffset())
	}
	if cur.Next() {
		t.Fatalf("expected end")
	}
	if !cur.Prev() || cur.RecordOffset() != off2 {
		t.Fatalf("prev off=%d", cur.RecordOffset())
	}
	if !cur.Last() || cur.RecordOffset() != off3 {
		t.Fatalf("last off=%d", cur.RecordOffset())
	}
}
