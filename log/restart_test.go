package log

import (
	"testing"

	ibos "github.com/wilhasse/innodb-go/os"
)

func TestLogRestartPreservesLSN(t *testing.T) {
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
	ReserveAndWriteFast([]byte("abc"))
	if flushed := FlushUpTo(3); flushed != 3 {
		t.Fatalf("FlushUpTo=%d, want 3", flushed)
	}
	if System.file != nil {
		_ = ibos.FileClose(System.file)
	}
	System = nil

	Init()
	if err := InitErr(); err != nil {
		t.Fatalf("InitErr after restart: %v", err)
	}
	if System.lsn != 3 {
		t.Fatalf("lsn=%d, want 3", System.lsn)
	}
	if System.flushed != 3 {
		t.Fatalf("flushed=%d, want 3", System.flushed)
	}
}
