package log

import (
	"testing"

	ibos "github.com/wilhasse/innodb-go/os"
)

func TestLogFileHeaderCreateOpen(t *testing.T) {
	dir := t.TempDir()
	t.Cleanup(func() {
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
	if System == nil || System.file == nil {
		t.Fatalf("expected log file to be open")
	}
	if System.header.Magic != logFileMagic {
		t.Fatalf("expected magic %#x, got %#x", logFileMagic, System.header.Magic)
	}
	if System.header.Version != logFileVersion {
		t.Fatalf("expected version %d, got %d", logFileVersion, System.header.Version)
	}
	if System.header.FileSize == 0 {
		t.Fatalf("expected non-zero file size")
	}
	_ = ibos.FileClose(System.file)

	cfg, _ := currentConfig()
	file, hdr, err := openLogFile(cfg)
	if err != nil {
		t.Fatalf("openLogFile: %v", err)
	}
	if hdr.Magic != logFileMagic || hdr.Version != logFileVersion {
		t.Fatalf("unexpected header on reopen")
	}
	_ = ibos.FileClose(file)
}
