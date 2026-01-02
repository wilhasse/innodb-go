package mtr

import (
	"github.com/wilhasse/innodb-go/dyn"
	"github.com/wilhasse/innodb-go/log"
)

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
	mtrWriteLog(m)
	if m.Log != nil {
		m.Log.Free()
		m.Log = nil
	}
	m.Memo = nil
	m.Modifications = false
	m.NLogRecs = 0
	m.State = StateCommitted
}

func mtrWriteLog(m *Mtr) {
	if m == nil || m.Log == nil {
		return
	}
	if m.LogMode == LogNone || !m.Modifications || m.NLogRecs == 0 {
		return
	}
	if m.NLogRecs > 1 {
		MlogCatenateUlint(m, MlogMultiRecEnd, Mlog1Byte)
	} else if block := m.Log.FirstBlock(); block != nil && block.Used() > 0 {
		block.Data()[0] |= MlogSingleRecFlag
	}
	dataSize := m.Log.DataSize()
	if dataSize == 0 {
		return
	}
	log.ReserveAndOpen(dataSize)
	if m.LogMode == LogAll || m.LogMode == LogShortInserts {
		for block := m.Log.FirstBlock(); block != nil; block = m.Log.NextBlock(block) {
			data := block.Data()
			used := block.Used()
			if used > len(data) {
				used = len(data)
			}
			if used > 0 {
				log.WriteLow(data[:used])
			}
		}
	}
	log.Close()
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
