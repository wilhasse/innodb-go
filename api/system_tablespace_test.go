package api

import (
	"path/filepath"
	"testing"

	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/fsp"
	ibos "github.com/wilhasse/innodb-go/os"
	"github.com/wilhasse/innodb-go/ut"
)

func TestSystemTablespaceLifecycle(t *testing.T) {
	dir := t.TempDir()
	dataDir := dir + "/"
	path := filepath.Join(dataDir, "ibdata1")
	dataFilePath := "ibdata1:4M:autoextend"
	wantPages := uint64((4 << 20) / ut.UNIV_PAGE_SIZE)

	initialized = false
	started = false
	activeDBFormat = ""

	if err := Init(); err != DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	if err := CfgSet("data_home_dir", dataDir); err != DB_SUCCESS {
		t.Fatalf("CfgSet data_home_dir: %v", err)
	}
	if err := CfgSet("data_file_path", dataFilePath); err != DB_SUCCESS {
		t.Fatalf("CfgSet data_file_path: %v", err)
	}
	if err := Startup(""); err != DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}

	exists, err := ibos.FileExists(path)
	if err != nil || !exists {
		t.Fatalf("ibdata1 missing: exists=%v err=%v", exists, err)
	}
	if got := fil.SpaceGetSize(0); got < wantPages {
		t.Fatalf("space size=%d, want >= %d", got, wantPages)
	}
	file := fil.SpaceGetFile(0)
	if file == nil {
		t.Fatalf("expected system tablespace file handle")
	}
	page, err := fil.ReadPage(file, 0)
	if err != nil {
		t.Fatalf("ReadPage: %v", err)
	}
	if got := fsp.GetSizeLow(page); got != uint32(wantPages) {
		t.Fatalf("header size=%d, want %d", got, wantPages)
	}
	if err := Shutdown(ShutdownNormal); err != DB_SUCCESS {
		t.Fatalf("Shutdown: %v", err)
	}

	if err := Init(); err != DB_SUCCESS {
		t.Fatalf("Init restart: %v", err)
	}
	if err := CfgSet("data_home_dir", dataDir); err != DB_SUCCESS {
		t.Fatalf("CfgSet data_home_dir restart: %v", err)
	}
	if err := CfgSet("data_file_path", dataFilePath); err != DB_SUCCESS {
		t.Fatalf("CfgSet data_file_path restart: %v", err)
	}
	if err := Startup(""); err != DB_SUCCESS {
		t.Fatalf("Startup restart: %v", err)
	}
	if got := fil.SpaceGetSize(0); got < wantPages {
		t.Fatalf("space size restart=%d, want >= %d", got, wantPages)
	}
	if err := Shutdown(ShutdownNormal); err != DB_SUCCESS {
		t.Fatalf("Shutdown restart: %v", err)
	}
}

func TestSystemTablespaceDefaultPathOnEmptySpec(t *testing.T) {
	dir := t.TempDir()
	dataDir := dir + "/"
	path := filepath.Join(dataDir, "ibdata1")

	initialized = false
	started = false
	activeDBFormat = ""

	if err := Init(); err != DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	if err := CfgSet("data_home_dir", dataDir); err != DB_SUCCESS {
		t.Fatalf("CfgSet data_home_dir: %v", err)
	}
	if err := CfgSet("data_file_path", ""); err != DB_SUCCESS {
		t.Fatalf("CfgSet data_file_path empty: %v", err)
	}
	if err := Startup(""); err != DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}

	exists, err := ibos.FileExists(path)
	if err != nil || !exists {
		t.Fatalf("ibdata1 missing: exists=%v err=%v", exists, err)
	}
	if got := fil.SpaceGetSize(0); got == 0 {
		t.Fatalf("space size=%d, want >0", got)
	}
	if err := Shutdown(ShutdownNormal); err != DB_SUCCESS {
		t.Fatalf("Shutdown: %v", err)
	}
}
