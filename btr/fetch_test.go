package btr

import (
	"bytes"
	"path/filepath"
	"testing"

	ibos "github.com/wilhasse/innodb-go/os"

	"github.com/wilhasse/innodb-go/buf"
	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/fsp"
	"github.com/wilhasse/innodb-go/ut"
)

func TestPageFetchReadsFromFile(t *testing.T) {
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

	data := make([]byte, ut.UNIV_PAGE_SIZE)
	for i := range data {
		data[i] = byte(i % 251)
	}
	if err := fil.SpaceWritePage(1, 0, data); err != nil {
		t.Fatalf("write page: %v", err)
	}

	oldPool := buf.GetDefaultPool()
	pool := buf.NewPool(1, ut.UnivPageSize)
	buf.SetDefaultPool(pool)
	defer buf.SetDefaultPool(oldPool)

	got, page, err := PageFetch(1, 0)
	if err != nil {
		t.Fatalf("page fetch: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("page mismatch")
	}
	PageRelease(page)
}
