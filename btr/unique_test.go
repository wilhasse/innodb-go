package btr

import (
	"testing"

	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/ut"
)

func TestUniqueCheckBytes(t *testing.T) {
	pg := make([]byte, ut.UNIV_PAGE_SIZE)
	recA := mustEncodeVarTuple("a")
	recB := mustEncodeVarTuple("b")
	if _, ok := page.InsertRecordBytes(pg, recA); !ok {
		t.Fatalf("insert failed")
	}
	if !UniqueCheckBytes(pg, recA, 1) {
		t.Fatalf("expected duplicate")
	}
	if UniqueCheckBytes(pg, recB, 1) {
		t.Fatalf("unexpected duplicate")
	}
}
