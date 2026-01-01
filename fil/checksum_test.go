package fil

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/wilhasse/innodb-go/mach"
	ibos "github.com/wilhasse/innodb-go/os"
	"github.com/wilhasse/innodb-go/ut"
)

func TestPageChecksumValidation(t *testing.T) {
	SetChecksumsEnabled(true)
	dir := t.TempDir()
	path := filepath.Join(dir, "page.ibd")
	file, err := ibos.FileCreateSimple(path, ibos.FileOverwrite, ibos.FileReadWrite)
	if err != nil {
		t.Fatalf("FileCreateSimple: %v", err)
	}
	defer ibos.FileClose(file)

	page := make([]byte, ut.UNIV_PAGE_SIZE)
	page[123] = 0xAA
	if err := WritePage(file, 0, page); err != nil {
		t.Fatalf("WritePage: %v", err)
	}
	if _, err := ReadPage(file, 0); err != nil {
		t.Fatalf("ReadPage: %v", err)
	}
	if _, err := ibos.FileWriteAt(file, []byte{0xBB}, 200); err != nil {
		t.Fatalf("FileWriteAt: %v", err)
	}
	if _, err := ReadPage(file, 0); err == nil || !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected checksum error, got %v", err)
	}
}

func TestPageLSNPersistence(t *testing.T) {
	SetChecksumsEnabled(true)
	dir := t.TempDir()
	path := filepath.Join(dir, "page_lsn.ibd")
	file, err := ibos.FileCreateSimple(path, ibos.FileOverwrite, ibos.FileReadWrite)
	if err != nil {
		t.Fatalf("FileCreateSimple: %v", err)
	}
	defer ibos.FileClose(file)

	page := make([]byte, ut.UNIV_PAGE_SIZE)
	mach.WriteUll(page[PageLSN:], 777)
	if err := WritePage(file, 0, page); err != nil {
		t.Fatalf("WritePage: %v", err)
	}
	read, err := ReadPage(file, 0)
	if err != nil {
		t.Fatalf("ReadPage: %v", err)
	}
	if got := mach.ReadUll(read[PageLSN:]); got != 777 {
		t.Fatalf("page LSN=%d, want 777", got)
	}
}
