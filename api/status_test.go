package api

import (
	"testing"

	"github.com/wilhasse/innodb-go/srv"
)

func TestStatusGetI64Unknown(t *testing.T) {
	var dst int64
	if err := StatusGetI64("missing_status", &dst); err != DB_NOT_FOUND {
		t.Fatalf("StatusGetI64 got %v, want %v", err, DB_NOT_FOUND)
	}
}

func TestStatusGetI64Value(t *testing.T) {
	srv.ExportVars.InnodbDataPendingReads = 42
	var dst int64
	if err := StatusGetI64("read_req_pending", &dst); err != DB_SUCCESS {
		t.Fatalf("StatusGetI64 got %v, want %v", err, DB_SUCCESS)
	}
	if dst != 42 {
		t.Fatalf("StatusGetI64 value %d, want 42", dst)
	}
}

func TestStatusGetI64Bool(t *testing.T) {
	srv.ExportVars.InnodbHaveAtomicBuiltins = IBTrue
	var dst int64
	if err := StatusGetI64("have_atomic_builtins", &dst); err != DB_SUCCESS {
		t.Fatalf("StatusGetI64 got %v, want %v", err, DB_SUCCESS)
	}
	if dst != 1 {
		t.Fatalf("StatusGetI64 value %d, want 1", dst)
	}
}

func TestStatusGetI64NilDst(t *testing.T) {
	if err := StatusGetI64("read_req_pending", nil); err != DB_INVALID_INPUT {
		t.Fatalf("StatusGetI64 got %v, want %v", err, DB_INVALID_INPUT)
	}
}
