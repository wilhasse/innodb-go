package log

import (
	"testing"

	ibos "github.com/wilhasse/innodb-go/os"
)

func TestLogFileAppendWrites(t *testing.T) {
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
	if System == nil || System.file == nil {
		t.Fatalf("expected log file to be open")
	}

	end, start := ReserveAndWriteFast([]byte("abc"))
	if start != 0 || end != 3 {
		t.Fatalf("unexpected lsn start=%d end=%d", start, end)
	}
	ReserveAndWriteFast([]byte("de"))

	buf := make([]byte, 5)
	if _, err := ibos.FileReadAt(System.file, buf, int64(logHeaderSize)); err != nil {
		t.Fatalf("FileReadAt: %v", err)
	}
	if got := string(buf); got != "abcde" {
		t.Fatalf("log bytes=%q, want %q", got, "abcde")
	}
	if System.fileSize < uint64(logHeaderSize+5) {
		t.Fatalf("expected file size >= %d, got %d", logHeaderSize+5, System.fileSize)
	}
}
