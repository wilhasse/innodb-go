package api

import (
	"testing"

	"github.com/wilhasse/innodb-go/buf"
	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/ut"
)

func TestShutdownFlushesBufferPool(t *testing.T) {
	dir := t.TempDir()
	dataDir := dir + "/"
	dataFilePath := "ibdata1:4M:autoextend"

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
	if err := CfgSet("buffer_pool_size", 2*ut.UNIV_PAGE_SIZE); err != DB_SUCCESS {
		t.Fatalf("CfgSet buffer_pool_size: %v", err)
	}
	if err := Startup(""); err != DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}

	pool := buf.GetDefaultPool()
	if pool == nil {
		t.Fatalf("expected buffer pool")
	}
	page, _, err := pool.Fetch(0, 1)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	page.Data[128] = 0xCC
	pool.MarkDirty(page)
	pool.Release(page)

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

	pageData, err := fil.SpaceReadPage(0, 1)
	if err != nil {
		t.Fatalf("SpaceReadPage: %v", err)
	}
	if len(pageData) == 0 || pageData[128] != 0xCC {
		t.Fatalf("expected persisted page data after restart")
	}
	if err := Shutdown(ShutdownNormal); err != DB_SUCCESS {
		t.Fatalf("Shutdown restart: %v", err)
	}
}
