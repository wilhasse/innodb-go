package fsp

import (
	"path/filepath"
	"testing"

	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/ut"
)

func TestExtentAllocRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ibdata1")
	sizeBytes := uint64(2*ExtentSize) * ut.UNIV_PAGE_SIZE

	fil.VarInit()
	Init()
	if !fil.SpaceCreate("system", 0, 0, fil.SpaceTablespace) {
		t.Fatalf("expected system space create")
	}
	if err := OpenSystemTablespace(SystemTablespaceSpec{
		Path:      path,
		SizeBytes: sizeBytes,
	}); err != nil {
		t.Fatalf("OpenSystemTablespace: %v", err)
	}

	p1 := AllocPage(0)
	p2 := AllocPage(0)
	if p1 == 0 || p2 == 0 {
		t.Fatalf("expected allocations beyond header page, got %d and %d", p1, p2)
	}
	FreePage(0, p1)
	if err := CloseSystemTablespace(); err != nil {
		t.Fatalf("CloseSystemTablespace: %v", err)
	}

	fil.VarInit()
	Init()
	if !fil.SpaceCreate("system", 0, 0, fil.SpaceTablespace) {
		t.Fatalf("expected system space create on restart")
	}
	if err := OpenSystemTablespace(SystemTablespaceSpec{
		Path:      path,
		SizeBytes: sizeBytes,
	}); err != nil {
		t.Fatalf("OpenSystemTablespace restart: %v", err)
	}
	p3 := AllocPage(0)
	if p3 != p1 {
		t.Fatalf("expected reuse of freed page %d, got %d", p1, p3)
	}
	if err := CloseSystemTablespace(); err != nil {
		t.Fatalf("CloseSystemTablespace restart: %v", err)
	}
}
