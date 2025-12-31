package os

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestFileCreateReadWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "file.dat")
	file, err := FileCreateSimple(path, FileCreatePath, FileReadWrite)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer FileClose(file)

	data := []byte("hello")
	if _, err := FileWriteAt(file, data, 128); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := FileFlush(file); err != nil {
		t.Fatalf("flush: %v", err)
	}
	FileClose(file)

	file, err = FileCreateSimple(path, FileOpen, FileReadWrite)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer FileClose(file)

	buf := make([]byte, len(data))
	if _, err := FileReadAt(file, buf, 128); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Fatalf("read mismatch: %q", buf)
	}
}

func TestFileOverwrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.dat")
	file, err := FileCreateSimple(path, FileCreate, FileReadWrite)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := FileWriteAt(file, []byte("data"), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	FileClose(file)

	file, err = FileCreateSimple(path, FileOverwrite, FileReadWrite)
	if err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	size, err := FileSize(file)
	if err != nil {
		t.Fatalf("size: %v", err)
	}
	if size != 0 {
		t.Fatalf("expected truncation, size=%d", size)
	}
	FileClose(file)
}

func TestFileExistsAndDelete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.dat")
	exists, err := FileExists(path)
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if exists {
		t.Fatalf("expected not exists")
	}
	file, err := FileCreateSimple(path, FileCreate, FileReadWrite)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	FileClose(file)
	exists, err = FileExists(path)
	if err != nil || !exists {
		t.Fatalf("expected exists")
	}
	if err := FileDelete(path); err != nil {
		t.Fatalf("delete: %v", err)
	}
	exists, err = FileExists(path)
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if exists {
		t.Fatalf("expected deleted")
	}
}
