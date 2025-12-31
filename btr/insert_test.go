package btr

import (
	"bytes"
	"testing"

	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/ut"
)

func TestLeafInsertBytes(t *testing.T) {
	pg := make([]byte, ut.UNIV_PAGE_SIZE)
	r1 := []byte{0x00, 0x01, 0x01, 0x01, 0x01}
	r2 := []byte{0x00, 0x02, 0x02, 0x02, 0x02}

	off1, ok := LeafInsertBytes(pg, r1)
	if !ok {
		t.Fatalf("insert r1 failed")
	}
	off2, ok := LeafInsertBytes(pg, r2)
	if !ok {
		t.Fatalf("insert r2 failed")
	}
	if got := pg[int(off1) : int(off1)+len(r1)]; !bytes.Equal(got, r1) {
		t.Fatalf("r1=%v", got)
	}
	if got := pg[int(off2) : int(off2)+len(r2)]; !bytes.Equal(got, r2) {
		t.Fatalf("r2=%v", got)
	}

	cur := page.NewSlotCursor(pg)
	if !cur.First() || cur.RecordOffset() != off1 {
		t.Fatalf("first off=%d", cur.RecordOffset())
	}
	if !cur.Next() || cur.RecordOffset() != off2 {
		t.Fatalf("next off=%d", cur.RecordOffset())
	}
}
