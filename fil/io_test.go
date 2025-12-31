package fil

import (
	"bytes"
	"path/filepath"
	"testing"

	ibos "github.com/wilhasse/innodb-go/os"
	"github.com/wilhasse/innodb-go/ut"
)

func TestReadWritePage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.ibd")

	file, err := ibos.FileCreateSimple(path, ibos.FileOverwrite, ibos.FileReadWrite)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer ibos.FileClose(file)

	page := make([]byte, ut.UNIV_PAGE_SIZE)
	for i := range page {
		page[i] = byte(i % 251)
	}
	if err := WritePage(file, 0, page); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := ReadPage(file, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, page) {
		t.Fatalf("page mismatch")
	}
}
