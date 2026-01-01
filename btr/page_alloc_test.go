package btr

import (
	"path/filepath"
	"testing"

	ibos "github.com/wilhasse/innodb-go/os"

	"github.com/wilhasse/innodb-go/buf"
	"github.com/wilhasse/innodb-go/dict"
	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/fsp"
	"github.com/wilhasse/innodb-go/mach"
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

func TestPageAllocWritesZeroPage(t *testing.T) {
	oldRegistry := page.PageRegistry
	page.PageRegistry = page.NewRegistry()
	defer func() {
		page.PageRegistry = oldRegistry
	}()

	oldPool := buf.GetDefaultPool()
	pool := buf.NewPool(2, ut.UnivPageSize)
	buf.SetDefaultPool(pool)
	defer buf.SetDefaultPool(oldPool)

	fil.VarInit()
	fsp.Init()
	if !fil.SpaceCreate("ts1", 1, 0, fil.SpaceTablespace) {
		t.Fatalf("expected space create")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "ts1.ibd")
	file, err := ibos.FileCreateSimple(path, ibos.FileOverwrite, ibos.FileReadWrite)
	if err != nil {
		t.Fatalf("file open: %v", err)
	}
	defer func() {
		_ = ibos.FileClose(file)
	}()
	if err := fil.SpaceSetFile(1, file); err != nil {
		t.Fatalf("space set file: %v", err)
	}

	idx := &dict.Index{Name: "idx", SpaceID: 1}
	p := PageAlloc(idx)
	if p == nil {
		t.Fatalf("expected page alloc")
	}
	pageBytes, err := fil.ReadPage(file, p.PageNo)
	if err != nil {
		t.Fatalf("read page: %v", err)
	}
	mach.WriteTo4(pageBytes[fil.PageSpaceOrChecksum:], 0)
	mach.WriteUll(pageBytes[fil.PageLSN:], 0)
	for i, b := range pageBytes {
		if b != 0 {
			t.Fatalf("expected zero page at byte %d, got %d", i, b)
		}
	}
}
