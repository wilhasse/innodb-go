package mtr

import (
	"testing"

	"github.com/wilhasse/innodb-go/log"
	"github.com/wilhasse/innodb-go/mach"
	"github.com/wilhasse/innodb-go/ut"
)

func TestMtrStartDefaults(t *testing.T) {
	m := &Mtr{}
	Start(m)
	if m.Log == nil {
		t.Fatalf("expected log to be initialized")
	}
	if m.LogMode != LogAll {
		t.Fatalf("log mode=%v", m.LogMode)
	}
	if m.State != StateActive {
		t.Fatalf("state=%v", m.State)
	}
	if m.NLogRecs != 0 || m.Modifications {
		t.Fatalf("expected clean start")
	}
}

func TestMtrLogMode(t *testing.T) {
	m := &Mtr{}
	Start(m)
	old := SetLogMode(m, LogNone)
	if old != LogAll || m.LogMode != LogNone {
		t.Fatalf("SetLogMode mismatch")
	}
	old = SetLogMode(m, LogShortInserts)
	if old != LogNone || m.LogMode != LogNone {
		t.Fatalf("expected LogNone to stay when switching to short inserts")
	}
}

func TestMtrMemoSavepoint(t *testing.T) {
	m := &Mtr{}
	Start(m)
	obj1 := &struct{}{}
	obj2 := &struct{}{}
	MemoPush(m, obj1, MemoSLock)
	sp := SetSavepoint(m)
	MemoPush(m, obj2, MemoXLock)
	if !MemoContains(m, obj2, MemoXLock) {
		t.Fatalf("expected obj2 in memo")
	}
	RollbackToSavepoint(m, sp)
	if MemoContains(m, obj2, MemoXLock) {
		t.Fatalf("expected obj2 to be rolled back")
	}
	if !MemoContains(m, obj1, MemoSLock) {
		t.Fatalf("expected obj1 to remain")
	}
}

func TestMtrCommit(t *testing.T) {
	m := &Mtr{}
	Start(m)
	m.Modifications = true
	m.NLogRecs = 1
	Commit(m)
	if m.State != StateCommitted {
		t.Fatalf("state=%v", m.State)
	}
	if m.Log != nil || m.Memo != nil {
		t.Fatalf("expected buffers cleared")
	}
}

func TestMtrCommitWritesLog(t *testing.T) {
	log.Init()
	m := &Mtr{}
	Start(m)
	page := makeTestPage(1, 2)
	MlogWriteUlint(page, 32, 0x1234, Mlog2Bytes, m)

	Commit(m)

	entries := log.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected one log entry, got %d", len(entries))
	}
	if len(entries[0].Data) == 0 || entries[0].Data[0]&MlogSingleRecFlag == 0 {
		t.Fatalf("expected single-rec flag on log entry")
	}
	if flushed := log.FlushUpTo(0); flushed != log.CurrentLSN() {
		t.Fatalf("expected flushed=%d, got %d", log.CurrentLSN(), flushed)
	}
}

func TestMtrCommitLogModeNone(t *testing.T) {
	log.Init()
	m := &Mtr{}
	Start(m)
	SetLogMode(m, LogNone)
	page := makeTestPage(3, 4)
	MlogWriteUlint(page, 40, 0xBEEF, Mlog2Bytes, m)
	Commit(m)

	if len(log.Entries()) != 0 {
		t.Fatalf("expected no log entries")
	}
	if flushed := log.FlushUpTo(0); flushed != 0 {
		t.Fatalf("expected flushed 0, got %d", flushed)
	}
}

func TestMtrCommitLogModeShortInserts(t *testing.T) {
	log.Init()
	m := &Mtr{}
	Start(m)
	SetLogMode(m, LogShortInserts)
	page := makeTestPage(5, 6)
	MlogWriteUlint(page, 48, 0xCAFE, Mlog2Bytes, m)
	Commit(m)

	if len(log.Entries()) != 1 {
		t.Fatalf("expected one log entry")
	}
	if flushed := log.FlushUpTo(0); flushed != 0 {
		t.Fatalf("expected no flush for short inserts, got %d", flushed)
	}
}

func TestMtrCommitMultiRecEnd(t *testing.T) {
	log.Init()
	m := &Mtr{}
	Start(m)
	page := makeTestPage(7, 8)
	MlogWriteUlint(page, 56, 0x1111, Mlog2Bytes, m)
	MlogWriteUlint(page, 60, 0x2222, Mlog2Bytes, m)
	Commit(m)

	entries := log.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected one log entry")
	}
	if got := entries[0].Data[len(entries[0].Data)-1]; got != MlogMultiRecEnd {
		t.Fatalf("expected multi-rec end flag, got %d", got)
	}
}

func makeTestPage(space, pageNo uint32) []byte {
	page := make([]byte, ut.UNIV_PAGE_SIZE)
	mach.WriteTo4(page[filPageArchLogNoOrSpaceID:], space)
	mach.WriteTo4(page[filPageOffset:], pageNo)
	return page
}
