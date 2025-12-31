package btr

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/rec"
	"github.com/wilhasse/innodb-go/ut"
)

func TestSearchRecordBytes(t *testing.T) {
	pg := make([]byte, ut.UNIV_PAGE_SIZE)
	recs := [][]byte{
		mustEncodeVarTuple("a"),
		mustEncodeVarTuple("c"),
		mustEncodeVarTuple("e"),
	}
	for _, recBytes := range recs {
		if _, ok := page.InsertRecordBytes(pg, recBytes); !ok {
			t.Fatalf("insert failed")
		}
	}

	keyB := mustEncodeVarTuple("b")
	off, exact := SearchRecordBytes(pg, keyB, 1)
	if exact {
		t.Fatalf("expected non-exact")
	}
	keyC := mustEncodeVarTuple("c")
	offC, exact := SearchRecordBytes(pg, keyC, 1)
	if !exact || offC == 0 {
		t.Fatalf("expected exact match")
	}
	if off == 0 || off != offC {
		t.Fatalf("expected GE to land on c")
	}

	keyZ := mustEncodeVarTuple("z")
	offZ, exact := SearchRecordBytes(pg, keyZ, 1)
	if exact || offZ != 0 {
		t.Fatalf("expected miss")
	}
}

func mustEncodeVarTuple(val string) []byte {
	tpl := &data.Tuple{Fields: []data.Field{{Data: []byte(val), Len: uint32(len(val))}}}
	recBytes, err := rec.EncodeVar(tpl, nil, 0)
	if err != nil {
		panic(err)
	}
	return recBytes
}
