package page

import (
	"testing"

	"github.com/wilhasse/innodb-go/ut"
)

func TestFreeListAddRemove(t *testing.T) {
	page := make([]byte, ut.UNIV_PAGE_SIZE)

	FreeListPush(page, 100, 10)
	if got := FreeListHead(page); got != 100 {
		t.Fatalf("head=%d", got)
	}
	if got := GarbageBytes(page); got != 10 {
		t.Fatalf("garbage=%d", got)
	}

	FreeListPush(page, 200, 8)
	if got := FreeListHead(page); got != 200 {
		t.Fatalf("head=%d", got)
	}
	if got := freeListNext(page, 200); got != 100 {
		t.Fatalf("next=%d", got)
	}
	if got := GarbageBytes(page); got != 18 {
		t.Fatalf("garbage=%d", got)
	}

	if got := FreeListPop(page, 8); got != 200 {
		t.Fatalf("pop=%d", got)
	}
	if got := FreeListHead(page); got != 100 {
		t.Fatalf("head=%d", got)
	}
	if got := GarbageBytes(page); got != 10 {
		t.Fatalf("garbage=%d", got)
	}
}
