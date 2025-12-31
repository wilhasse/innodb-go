package mtr

import "testing"

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
