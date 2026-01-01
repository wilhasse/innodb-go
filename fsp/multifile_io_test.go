package fsp

import (
	"path/filepath"
	"testing"

	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/ut"
)

func TestSystemTablespaceMultiFileIO(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "ibdata1")
	path2 := filepath.Join(dir, "ibdata2")
	sizePages := uint64(4)
	sizeBytes := sizePages * ut.UNIV_PAGE_SIZE

	fil.VarInit()
	Init()
	if !fil.SpaceCreate("system", 0, 0, fil.SpaceTablespace) {
		t.Fatalf("expected system space create")
	}
	if err := OpenSystemTablespace(SystemTablespaceSpec{
		Files: []TablespaceFileSpec{
			{Path: path1, SizeBytes: sizeBytes},
			{Path: path2, SizeBytes: sizeBytes},
		},
	}); err != nil {
		t.Fatalf("OpenSystemTablespace: %v", err)
	}

	pageNo := uint32(sizePages)
	data := make([]byte, ut.UNIV_PAGE_SIZE)
	data[0] = 0xAB
	if err := fil.SpaceWritePage(0, pageNo, data); err != nil {
		t.Fatalf("SpaceWritePage: %v", err)
	}
	read, err := fil.SpaceReadPage(0, pageNo)
	if err != nil {
		t.Fatalf("SpaceReadPage: %v", err)
	}
	if len(read) == 0 || read[0] != 0xAB {
		t.Fatalf("expected page data from second file")
	}
	if err := CloseSystemTablespace(); err != nil {
		t.Fatalf("CloseSystemTablespace: %v", err)
	}
}
