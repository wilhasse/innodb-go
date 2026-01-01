package fil

import (
	"path/filepath"
	"testing"

	iblog "github.com/wilhasse/innodb-go/log"
	"github.com/wilhasse/innodb-go/mach"
	ibos "github.com/wilhasse/innodb-go/os"
	"github.com/wilhasse/innodb-go/ut"
)

func TestSpaceReadAppliesRecvRecords(t *testing.T) {
	VarInit()
	iblog.RecvSysVarInit()
	iblog.RecvSysCreate()
	iblog.RecvSysInit(0)

	dir := t.TempDir()
	path := filepath.Join(dir, "space.ibd")
	file, err := ibos.FileCreateSimple(path, ibos.FileCreate, ibos.FileReadWrite)
	if err != nil {
		t.Fatalf("FileCreateSimple: %v", err)
	}
	defer ibos.FileClose(file)

	if !SpaceCreate("test", 1, 0, SpaceTablespace) {
		t.Fatalf("SpaceCreate failed")
	}
	if err := SpaceSetFile(1, file); err != nil {
		t.Fatalf("SpaceSetFile: %v", err)
	}

	page := make([]byte, ut.UNIV_PAGE_SIZE)
	mach.WriteUll(page[PageLSN:], 5)
	if err := WritePage(file, 0, page); err != nil {
		t.Fatalf("WritePage: %v", err)
	}

	iblog.RecvAddRecord(1, 0, 1, []byte("x"), 10, 30)
	buf := make([]byte, ut.UNIV_PAGE_SIZE)
	if err := SpaceReadPageInto(1, 0, buf); err != nil {
		t.Fatalf("SpaceReadPageInto: %v", err)
	}
	if got := mach.ReadUll(buf[PageLSN:]); got != 30 {
		t.Fatalf("page LSN=%d, want 30", got)
	}
	if iblog.RecvSysState.NAddrs != 0 {
		t.Fatalf("expected recv hash to be cleared")
	}
}
