package btr

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/rec"
	"github.com/wilhasse/innodb-go/ut"
)

func TestLeafInsertBytes(t *testing.T) {
	pg := make([]byte, ut.UNIV_PAGE_SIZE)
	r1 := mustEncodeVarRecord(t, "b")
	r2 := mustEncodeVarRecord(t, "a")

	if _, ok := LeafInsertBytes(pg, r1); !ok {
		t.Fatalf("insert r1 failed")
	}
	if _, ok := LeafInsertBytes(pg, r2); !ok {
		t.Fatalf("insert r2 failed")
	}
	if page.HeaderGetField(pg, page.PageNRecs) != 2 {
		t.Fatalf("n_recs=%d", page.HeaderGetField(pg, page.PageNRecs))
	}

	keyA := mustEncodeVarTuple("a")
	keyB := mustEncodeVarTuple("b")
	offA, exact := SearchRecordBytes(pg, keyA, 1)
	if !exact || offA == 0 {
		t.Fatalf("expected exact match for a")
	}
	offB, exact := SearchRecordBytes(pg, keyB, 1)
	if !exact || offB == 0 {
		t.Fatalf("expected exact match for b")
	}
	if offA >= offB {
		t.Fatalf("expected a before b")
	}

	cur := page.NewSlotCursor(pg)
	if !cur.First() || cur.RecordOffset() != offA {
		t.Fatalf("first off=%d", cur.RecordOffset())
	}
	if !cur.Next() || cur.RecordOffset() != offB {
		t.Fatalf("next off=%d", cur.RecordOffset())
	}
}

func TestLeafInsertBytesClearsGarbage(t *testing.T) {
	pg := make([]byte, ut.UNIV_PAGE_SIZE)
	if _, ok := LeafInsertBytes(pg, mustEncodeVarRecord(t, "a")); !ok {
		t.Fatalf("insert a failed")
	}
	if _, ok := LeafInsertBytes(pg, mustEncodeVarRecord(t, "b")); !ok {
		t.Fatalf("insert b failed")
	}

	keyA := mustEncodeVarTuple("a")
	if !DeleteRecordBytes(pg, keyA, 1) {
		t.Fatalf("delete failed")
	}
	if page.GarbageBytes(pg) == 0 {
		t.Fatalf("expected garbage")
	}
	if page.FreeListHead(pg) == 0 {
		t.Fatalf("expected free list head")
	}

	if _, ok := LeafInsertBytes(pg, mustEncodeVarRecord(t, "c")); !ok {
		t.Fatalf("insert c failed")
	}
	if page.GarbageBytes(pg) != 0 {
		t.Fatalf("garbage=%d", page.GarbageBytes(pg))
	}
	if page.FreeListHead(pg) != 0 {
		t.Fatalf("free head=%d", page.FreeListHead(pg))
	}
}

func mustEncodeVarRecord(t *testing.T, val string) []byte {
	t.Helper()
	tpl := &data.Tuple{Fields: []data.Field{{Data: []byte(val), Len: uint32(len(val))}}}
	recBytes, err := rec.EncodeVar(tpl, nil, recordExtra)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec.HeaderSetStatus(recBytes, rec.RecStatusOrdinary)
	rec.HeaderSetInfoBits(recBytes, rec.RecInfoMinRecFlag)
	return recBytes
}
