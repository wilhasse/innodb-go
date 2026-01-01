package log

import "testing"

func TestRecvSysVarInit(t *testing.T) {
	RecvRecoveryOn = true
	RecvNeededRecovery = true
	RecvLSNChecksOn = true
	RecvNoIbufOperations = true
	RecvNPoolFreeFrames = 10
	RecvSysVarInit()
	if RecvRecoveryOn || RecvNeededRecovery || RecvLSNChecksOn || RecvNoIbufOperations {
		t.Fatalf("expected recovery flags to reset")
	}
	if RecvNPoolFreeFrames != 256 {
		t.Fatalf("expected pool free frames to be 256")
	}
}

func TestRecvAddAndRecoverPage(t *testing.T) {
	RecvSysVarInit()
	RecvSysCreate()
	RecvSysInit(0)
	RecvAddRecord(1, 2, 3, []byte("x"), 10, 20)
	RecvAddRecord(1, 2, 4, []byte("y"), 21, 30)
	page := &Page{SpaceID: 1, PageNo: 2, LSN: 5}
	if !RecvRecoverPage(page) {
		t.Fatalf("expected page to recover")
	}
	if page.LSN != 30 {
		t.Fatalf("expected page LSN to advance to 30, got %d", page.LSN)
	}
	if RecvSysState.NAddrs != 0 {
		t.Fatalf("expected hash to be empty after recovery")
	}
}

func TestRecvScanLogRecs(t *testing.T) {
	RecvSysVarInit()
	RecvSysCreate()
	RecvSysInit(0)
	var contiguous uint64
	var scanned uint64
	buf := EncodeRecord(Record{
		Type:    1,
		SpaceID: 2,
		PageNo:  3,
		Payload: []byte("x"),
	})
	done := RecvScanLogRecs(true, buf, 100, &contiguous, &scanned)
	if !done {
		t.Fatalf("expected scan to finish")
	}
	if contiguous != 100+uint64(len(buf)) || scanned != 100+uint64(len(buf)) {
		t.Fatalf("unexpected scan lsn values")
	}
	if RecvSysState.NAddrs == 0 {
		t.Fatalf("expected record stored")
	}
}

func TestRecvRecoveryFromCheckpoint(t *testing.T) {
	RecvSysVarInit()
	RecvRecoveryFromCheckpointStart(RecoveryCrash, 10, 20)
	if !RecvRecoveryOn || !RecvNeededRecovery || !RecvLSNChecksOn {
		t.Fatalf("expected recovery flags to be set")
	}
	RecvRecoveryFromCheckpointFinish(RecoveryCrash)
	if RecvRecoveryOn || RecvNeededRecovery || RecvLSNChecksOn {
		t.Fatalf("expected recovery flags to be cleared")
	}
}

func TestRecvResetLogs(t *testing.T) {
	RecvResetLogs(123)
	if System == nil || System.lsn != 123 {
		t.Fatalf("expected log system reset to 123")
	}
}
