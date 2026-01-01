package log

import (
	"testing"

	ibos "github.com/wilhasse/innodb-go/os"
)

func TestCheckpointPersistence(t *testing.T) {
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
	FlushUpTo(3)
	if got := Checkpoint(); got != 3 {
		t.Fatalf("Checkpoint=%d, want 3", got)
	}
	if System.file != nil {
		_ = ibos.FileClose(System.file)
	}
	System = nil

	Init()
	if err := InitErr(); err != nil {
		t.Fatalf("InitErr after restart: %v", err)
	}
	if got := CheckpointLSN(); got != 3 {
		t.Fatalf("CheckpointLSN=%d, want 3", got)
	}
}
