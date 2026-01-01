package api

import (
	"testing"

	"github.com/wilhasse/innodb-go/buf"
	"github.com/wilhasse/innodb-go/dict"
	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/log"
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
	if err := Startup("barracuda"); err != DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}
	log.ReserveAndWriteFast(log.EncodeRecord(log.Record{
		Type:    1,
		SpaceID: 1,
		PageNo:  1,
		Payload: []byte("x"),
	}))
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
	if err := Startup("barracuda"); err != DB_SUCCESS {
		t.Fatalf("Startup after crash: %v", err)
	}
	if log.RecvSysState == nil || log.RecvSysState.NAddrs == 0 {
		t.Fatalf("expected recovery scan to populate recv hash")
	}
	_ = Shutdown(ShutdownNormal)
}
