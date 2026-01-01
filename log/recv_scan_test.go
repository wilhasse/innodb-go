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

	ReserveAndWriteFast(buildMlogStringRecord(10, 11, 4, []byte("a")))
	ReserveAndWriteFast(buildMlogStringRecord(12, 13, 4, []byte("b")))
	FlushUpTo(System.lsn)

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
