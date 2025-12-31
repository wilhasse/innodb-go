package api

import (
	stdos "os"
	"testing"
)

func TestCreateTempFile(t *testing.T) {
	file, errCode := CreateTempFile("ibgo")
	if errCode != DB_SUCCESS {
		t.Fatalf("CreateTempFile got %v, want %v", errCode, DB_SUCCESS)
	}
	if file == nil {
		t.Fatal("CreateTempFile returned nil file")
	}
	name := file.Name()
	if err := file.Close(); err != nil {
		t.Fatalf("Close temp file: %v", err)
	}
	if err := removeFile(name); err != nil {
		t.Fatalf("remove temp file: %v", err)
	}
}

func TestTrxIsInterrupted(t *testing.T) {
	if got := TrxIsInterrupted(nil); got != IBFalse {
		t.Fatalf("TrxIsInterrupted got %v, want %v", got, IBFalse)
	}
}

func removeFile(path string) error {
	return stdos.Remove(path)
}
