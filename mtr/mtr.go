package mtr

import "github.com/wilhasse/innodb-go/dyn"

// Start initializes a mini-transaction in the provided buffer.
func Start(m *Mtr) *Mtr {
	if m == nil {
		return nil
	}
	if m.Log != nil {
		m.Log.Free()
	}
	m.Log = dyn.New()
	m.LogMode = LogAll
	m.Modifications = false
	m.NLogRecs = 0
	m.State = StateActive
	m.Memo = m.Memo[:0]
	return m
}

// Commit finalizes the mini-transaction and clears buffers.
func Commit(m *Mtr) {
	if m == nil {
		return
	}
	m.State = StateCommitting
	if m.Log != nil {
		m.Log.Free()
		m.Log = nil
	}
	m.Memo = nil
	m.Modifications = false
	m.NLogRecs = 0
	m.State = StateCommitted
}

// GetLogMode returns the current logging mode.
func GetLogMode(m *Mtr) LogMode {
	if m == nil {
		return LogNone
	}
	return m.LogMode
}

// SetLogMode changes the logging mode and returns the previous value.
func SetLogMode(m *Mtr, mode LogMode) LogMode {
	if m == nil {
		return LogNone
	}
	old := m.LogMode
	if mode == LogShortInserts && old == LogNone {
		return old
	}
	m.LogMode = mode
	return old
}

// MemoPush records an object in the memo stack.
func MemoPush(m *Mtr, object any, typ MemoType) {
	if m == nil || object == nil {
		return
	}
	m.Memo = append(m.Memo, MemoSlot{Object: object, Type: typ})
}

// SetSavepoint returns the current memo stack size.
func SetSavepoint(m *Mtr) int {
	if m == nil {
		return 0
	}
	return len(m.Memo)
}

// RollbackToSavepoint discards memo entries after the savepoint.
func RollbackToSavepoint(m *Mtr, savepoint int) {
	if m == nil {
		return
	}
	if savepoint < 0 {
		savepoint = 0
	}
	if savepoint > len(m.Memo) {
		return
	}
	m.Memo = m.Memo[:savepoint]
}

// MemoContains reports whether the memo stack contains an object/type pair.
func MemoContains(m *Mtr, object any, typ MemoType) bool {
	if m == nil {
		return false
	}
	for _, slot := range m.Memo {
		if slot.Object == object && slot.Type == typ {
			return true
		}
	}
	return false
}
