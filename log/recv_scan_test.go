package log

import (
	"testing"

	ibos "github.com/wilhasse/innodb-go/os"
)

func TestRecvScanLogFile(t *testing.T) {
	dir := t.TempDir()
	t.Cleanup(func() {
		if System != nil && System.file != nil {
			_ = ibos.FileClose(System.file)
		}
		config = Config{}
		configSet = false
		System = nil
	})
	Configure(Config{
		Enabled:  true,
		DataDir:  dir,
		FileSize: 1 << 20,
	})
	Init()
	if err := InitErr(); err != nil {
		t.Fatalf("InitErr: %v", err)
	}

	RecvSysVarInit()
	RecvSysCreate()
	RecvSysInit(0)

	ReserveAndWriteFast(EncodeRecord(Record{Type: 1, SpaceID: 10, PageNo: 11, Payload: []byte("a")}))
	ReserveAndWriteFast(EncodeRecord(Record{Type: 2, SpaceID: 12, PageNo: 13, Payload: []byte("b")}))

	contiguous, scanned, err := RecvScanLogFile(System.file, 0, System.lsn)
	if err != nil {
		t.Fatalf("RecvScanLogFile: %v", err)
	}
	if contiguous != System.lsn || scanned != System.lsn {
		t.Fatalf("contiguous=%d scanned=%d want=%d", contiguous, scanned, System.lsn)
	}
	if RecvSysState.NAddrs != 2 {
		t.Fatalf("expected 2 recv addrs, got %d", RecvSysState.NAddrs)
	}
}
