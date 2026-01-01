package fsp

import (
	"path/filepath"
	"testing"

	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/ut"
)

func TestNodeMetadataReload(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "ibdata1")
	path2 := filepath.Join(dir, "ibdata2")
	size1 := uint64(4) * ut.UNIV_PAGE_SIZE
	size2 := uint64(3) * ut.UNIV_PAGE_SIZE

	fil.VarInit()
	Init()
	if !fil.SpaceCreate("system", 0, 0, fil.SpaceTablespace) {
		t.Fatalf("expected system space create")
	}
	if err := OpenSystemTablespace(SystemTablespaceSpec{
		Files: []TablespaceFileSpec{
			{Path: path1, SizeBytes: size1},
			{Path: path2, SizeBytes: size2},
		},
	}); err != nil {
		t.Fatalf("OpenSystemTablespace: %v", err)
	}
	if err := CloseSystemTablespace(); err != nil {
		t.Fatalf("CloseSystemTablespace: %v", err)
	}

	fil.VarInit()
	Init()
	if !fil.SpaceCreate("system", 0, 0, fil.SpaceTablespace) {
		t.Fatalf("expected system space create after restart")
	}
	if err := OpenSystemTablespace(SystemTablespaceSpec{
		Files: []TablespaceFileSpec{
			{Path: path1, SizeBytes: ut.UNIV_PAGE_SIZE},
			{Path: path2, SizeBytes: ut.UNIV_PAGE_SIZE},
		},
	}); err != nil {
		t.Fatalf("OpenSystemTablespace restart: %v", err)
	}
	if got := fil.SpaceGetSize(0); got != 7 {
		t.Fatalf("space size=%d, want 7", got)
	}
	if err := CloseSystemTablespace(); err != nil {
		t.Fatalf("CloseSystemTablespace restart: %v", err)
	}
}
