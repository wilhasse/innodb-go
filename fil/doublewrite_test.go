package fil

import (
	"bytes"
	"path/filepath"
	"testing"

	ibos "github.com/wilhasse/innodb-go/os"
	"github.com/wilhasse/innodb-go/ut"
)

func TestDoublewriteRecoverRestoresPage(t *testing.T) {
	VarInit()
	SetDoublewriteEnabled(true)

	dir := t.TempDir()
	if err := DoublewriteInit(dir); err != nil {
		t.Fatalf("DoublewriteInit: %v", err)
	}
	t.Cleanup(DoublewriteShutdown)

	path := filepath.Join(dir, "space.ibd")
	file, err := ibos.FileCreateSimple(path, ibos.FileCreate, ibos.FileReadWrite)
	if err != nil {
		t.Fatalf("FileCreateSimple: %v", err)
	}
	defer ibos.FileClose(file)

	if !SpaceCreate("dblwr", 2, 0, SpaceTablespace) {
		t.Fatalf("SpaceCreate failed")
	}
	if err := SpaceSetFile(2, file); err != nil {
		t.Fatalf("SpaceSetFile: %v", err)
	}

	page := make([]byte, ut.UNIV_PAGE_SIZE)
	copy(page[100:], []byte("doublewrite"))
	if err := SpaceWritePage(2, 0, page); err != nil {
		t.Fatalf("SpaceWritePage: %v", err)
	}

	zero := make([]byte, ut.UNIV_PAGE_SIZE)
	if _, err := ibos.FileWritePage(file, 0, zero); err != nil {
		t.Fatalf("FileWritePage: %v", err)
	}

	if err := DoublewriteRecover(); err != nil {
		t.Fatalf("DoublewriteRecover: %v", err)
	}

	got, err := SpaceReadPage(2, 0)
	if err != nil {
		t.Fatalf("SpaceReadPage: %v", err)
	}
	if !bytes.Equal(got[100:100+len("doublewrite")], []byte("doublewrite")) {
		t.Fatalf("expected page restored from doublewrite")
	}

	if size, err := ibos.FileSize(doublewriteFile); err == nil && size != 0 {
		t.Fatalf("expected doublewrite file to be truncated, size=%d", size)
	} else if err != nil {
		t.Fatalf("FileSize: %v", err)
	}
}
