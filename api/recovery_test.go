package api

import (
	"testing"

	"github.com/wilhasse/innodb-go/buf"
	"github.com/wilhasse/innodb-go/dict"
	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/log"
	"github.com/wilhasse/innodb-go/mach"
)

func TestStartupRunsRecoveryOnCrash(t *testing.T) {
	dir := t.TempDir() + "/"
	initialized = false
	started = false
	activeDBFormat = ""

	if err := Init(); err != DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	if err := CfgSet("data_home_dir", dir); err != DB_SUCCESS {
		t.Fatalf("CfgSet data_home_dir: %v", err)
	}
	if err := CfgSet("data_file_path", "ibdata1:4M:autoextend"); err != DB_SUCCESS {
		t.Fatalf("CfgSet data_file_path: %v", err)
	}
	if err := Startup("barracuda"); err != DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}
	pageNo := uint32(fil.SpaceGetSize(0) + 1000)
	log.ReserveAndWriteFast(buildMlogStringRecord(0, pageNo, 128, []byte("x")))
	log.FlushUpTo(log.CurrentLSN())

	log.CloseFileForCrash()
	log.System = nil

	initialized = false
	started = false
	activeDBFormat = ""
	resetSchemaState()
	buf.SetDefaultPool(nil)
	dict.DictClose()
	fil.VarInit()

	if err := Init(); err != DB_SUCCESS {
		t.Fatalf("Init after crash: %v", err)
	}
	if err := CfgSet("data_home_dir", dir); err != DB_SUCCESS {
		t.Fatalf("CfgSet data_home_dir after crash: %v", err)
	}
	if err := CfgSet("data_file_path", "ibdata1:4M:autoextend"); err != DB_SUCCESS {
		t.Fatalf("CfgSet data_file_path after crash: %v", err)
	}
	if err := Startup("barracuda"); err != DB_SUCCESS {
		t.Fatalf("Startup after crash: %v", err)
	}
	if log.RecvSysState == nil || log.RecvSysState.NAddrs == 0 {
		t.Fatalf("expected recovery scan to populate recv hash")
	}
	pageData, err := fil.SpaceReadPage(0, pageNo)
	if err != nil {
		t.Fatalf("SpaceReadPage: %v", err)
	}
	if len(pageData) == 0 || pageData[128] != 'x' {
		t.Fatalf("expected redo to apply to page data")
	}
	_ = Shutdown(ShutdownNormal)
}

func buildMlogStringRecord(space, pageNo uint32, offset int, data []byte) []byte {
	buf := make([]byte, 0, 16+len(data))
	buf = append(buf, 30)
	tmp := make([]byte, 10)
	n := mach.WriteCompressed(tmp, space)
	buf = append(buf, tmp[:n]...)
	n = mach.WriteCompressed(tmp, pageNo)
	buf = append(buf, tmp[:n]...)
	payload := make([]byte, 4+len(data))
	mach.WriteTo2(payload[0:], uint32(offset))
	mach.WriteTo2(payload[2:], uint32(len(data)))
	copy(payload[4:], data)
	buf = append(buf, payload...)
	return buf
}
