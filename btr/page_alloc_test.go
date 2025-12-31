package btr

import (
	"testing"

	"github.com/wilhasse/innodb-go/buf"
	"github.com/wilhasse/innodb-go/dict"
	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/fsp"
	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/ut"
)

func TestPageAllocFreeSize(t *testing.T) {
	oldRegistry := page.PageRegistry
	page.PageRegistry = page.NewRegistry()
	defer func() {
		page.PageRegistry = oldRegistry
	}()

	oldPool := buf.GetDefaultPool()
	pool := buf.NewPool(10, ut.UnivPageSize)
	buf.SetDefaultPool(pool)
	defer buf.SetDefaultPool(oldPool)

	fil.VarInit()
	fsp.Init()
	if !fil.SpaceCreate("ts1", 1, 0, fil.SpaceTablespace) {
		t.Fatalf("expected space create")
	}

	idx := &dict.Index{Name: "idx", SpaceID: 1}
	p1 := PageAlloc(idx)
	if p1 == nil {
		t.Fatalf("expected page alloc")
	}
	p2 := PageAlloc(idx)
	if p2 == nil {
		t.Fatalf("expected second page alloc")
	}

	if got := GetSize(idx); got != 2 {
		t.Fatalf("size mismatch: got %d want 2", got)
	}
	if stats := pool.Stats(); stats.Size != 2 {
		t.Fatalf("pool size mismatch: got %d want 2", stats.Size)
	}

	PageFree(idx, p1)
	if got := GetSize(idx); got != 1 {
		t.Fatalf("size after free mismatch: got %d want 1", got)
	}
	if stats := pool.Stats(); stats.Size != 1 {
		t.Fatalf("pool size after free mismatch: got %d want 1", stats.Size)
	}

	p3 := PageAlloc(idx)
	if p3 == nil {
		t.Fatalf("expected page alloc after free")
	}
	if p3.PageNo != p1.PageNo {
		t.Fatalf("expected reuse of freed page no, got %d want %d", p3.PageNo, p1.PageNo)
	}
	if got := GetSize(idx); got != 2 {
		t.Fatalf("size after reuse mismatch: got %d want 2", got)
	}
}
