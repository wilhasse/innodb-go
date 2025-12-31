package btr

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/rec"
	"github.com/wilhasse/innodb-go/ut"
)

func TestDeleteRecordBytesSkips(t *testing.T) {
	pg := make([]byte, ut.UNIV_PAGE_SIZE)
	recs := [][]byte{
		mustEncodeVarTuple("a"),
		mustEncodeVarTuple("b"),
		mustEncodeVarTuple("c"),
	}
	for _, recBytes := range recs {
		if _, ok := page.InsertRecordBytes(pg, recBytes); !ok {
			t.Fatalf("insert failed")
		}
	}

	if !DeleteRecordBytes(pg, recs[1], 1) {
		t.Fatalf("delete failed")
	}
	off, exact := SearchRecordBytes(pg, recs[1], 1)
	if exact || off == 0 {
		t.Fatalf("expected search to skip deleted")
	}
	if page.HeaderGetField(pg, page.PageNRecs) != 2 {
		t.Fatalf("n_recs=%d", page.HeaderGetField(pg, page.PageNRecs))
	}
}

func TestDeleteRecordBytesTriggersReorg(t *testing.T) {
	pg := make([]byte, ut.UNIV_PAGE_SIZE)
	largeVal := make([]byte, BtrCurPageReorganizeLimit+10)
	tpl := &data.Tuple{Fields: []data.Field{{Data: largeVal, Len: uint32(len(largeVal))}}}
	recBytes, err := rec.EncodeVar(tpl, nil, 0)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, ok := page.InsertRecordBytes(pg, recBytes); !ok {
		t.Fatalf("insert failed")
	}
	if !DeleteRecordBytes(pg, recBytes, 1) {
		t.Fatalf("delete failed")
	}
	if page.GarbageBytes(pg) != 0 {
		t.Fatalf("garbage=%d", page.GarbageBytes(pg))
	}
	if page.HeaderGetField(pg, page.PageNRecs) != 0 {
		t.Fatalf("n_recs=%d", page.HeaderGetField(pg, page.PageNRecs))
	}
}
