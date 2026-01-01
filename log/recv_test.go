package log

import (
	"bytes"
	"testing"

	"github.com/wilhasse/innodb-go/mach"
)

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
	payload1 := buildMlogStringPayload(8, []byte("x"))
	payload2 := buildMlogStringPayload(9, []byte("y"))
	RecvAddRecord(1, 2, mlogWriteStringType, payload1, 10, 20)
	RecvAddRecord(1, 2, mlogWriteStringType, payload2, 21, 30)
	page := make([]byte, 64)
	setPageLSN(page, 5)
	if !RecvRecoverPage(1, 2, page) {
		t.Fatalf("expected page to recover")
	}
	if !bytes.Equal(page[8:9], []byte("x")) || !bytes.Equal(page[9:10], []byte("y")) {
		t.Fatalf("expected payloads to apply to page")
	}
	if got := pageLSN(page); got != 30 {
		t.Fatalf("expected page LSN to advance to 30, got %d", got)
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
	buf := buildMlogStringRecord(2, 3, 4, []byte("x"))
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

func buildMlogStringPayload(offset int, data []byte) []byte {
	buf := make([]byte, 4+len(data))
	mach.WriteTo2(buf[0:], uint32(offset))
	mach.WriteTo2(buf[2:], uint32(len(data)))
	copy(buf[4:], data)
	return buf
}

func buildMlogStringRecord(space, pageNo uint32, offset int, data []byte) []byte {
	buf := make([]byte, 0, 16+len(data))
	buf = append(buf, mlogWriteStringType)
	tmp := make([]byte, 10)
	n := mach.WriteCompressed(tmp, space)
	buf = append(buf, tmp[:n]...)
	n = mach.WriteCompressed(tmp, pageNo)
	buf = append(buf, tmp[:n]...)
	payload := buildMlogStringPayload(offset, data)
	buf = append(buf, payload...)
	return buf
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
