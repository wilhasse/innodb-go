package srv

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/wilhasse/innodb-go/buf"
	"github.com/wilhasse/innodb-go/fil"
	ibos "github.com/wilhasse/innodb-go/os"
	"github.com/wilhasse/innodb-go/ut"
)

func TestPageCleanerFlushesDirtyPages(t *testing.T) {
	fil.VarInit()
	buf.SetDefaultPools(nil)
	defer buf.SetDefaultPools(nil)

	dir := t.TempDir()
	path := filepath.Join(dir, "cleaner.ibd")
	file, err := ibos.FileCreateSimple(path, ibos.FileCreate, ibos.FileReadWrite)
	if err != nil {
		t.Fatalf("FileCreateSimple: %v", err)
	}
	defer ibos.FileClose(file)

	if !fil.SpaceCreate("cleaner", 1, 0, fil.SpaceTablespace) {
		t.Fatalf("SpaceCreate failed")
	}
	if err := fil.SpaceSetFile(1, file); err != nil {
		t.Fatalf("SpaceSetFile: %v", err)
	}

	pool := buf.NewPool(2, ut.UNIV_PAGE_SIZE)
	buf.SetDefaultPool(pool)

	page, _, err := pool.Fetch(1, 0)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	page.Data[128] = 0x7f
	pool.MarkDirty(page)
	pool.Release(page)

	cleaner := NewPageCleaner(PageCleanerConfig{
		Interval:    10 * time.Millisecond,
		WorkerCount: 1,
	})
	if err := cleaner.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer cleaner.Stop()

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if pool.Stats().Dirty == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pool.Stats().Dirty != 0 {
		t.Fatalf("expected dirty pages to be flushed")
	}

	bufPage := make([]byte, ut.UNIV_PAGE_SIZE)
	if err := fil.SpaceReadPageInto(1, 0, bufPage); err != nil {
		t.Fatalf("SpaceReadPageInto: %v", err)
	}
	if bufPage[128] != 0x7f {
		t.Fatalf("expected flushed data in tablespace")
	}
}
