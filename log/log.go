package log

import (
	"sync"

	ibos "github.com/wilhasse/innodb-go/os"
)

// Entry holds a log record.
type Entry struct {
	StartLSN uint64
	EndLSN   uint64
	Data     []byte
}

// Log holds redo log state.
type Log struct {
	mu        sync.Mutex
	entries   []Entry
	lsn       uint64
	flushed   uint64
	startLSN  uint64
	checkpoint uint64
	fileSize uint64
	open      bool
	openStart uint64
	pending   []byte
	file      ibos.File
	header  logHeader
	initErr error
}

// System is the global redo log.
var System *Log

// Init initializes the global log system.
func Init() {
	System = &Log{}
	cfg, ok := currentConfig()
	if !ok || !cfg.Enabled {
		return
	}
	file, hdr, err := openLogFile(cfg)
	if err != nil {
		System.initErr = err
		return
	}
	System.file = file
	System.header = hdr
	System.startLSN = hdr.StartLSN
	System.checkpoint = hdr.CheckpointLSN
	System.lsn = hdr.CurrentLSN
	System.flushed = hdr.FlushedLSN
}

// InitErr returns the last initialization error.
func InitErr() error {
	if System == nil {
		return nil
	}
	return System.initErr
}

// Acquire locks the global log.
func Acquire() {
	if System == nil {
		Init()
	}
	System.mu.Lock()
}

// Release unlocks the global log.
func Release() {
	if System == nil {
		return
	}
	System.mu.Unlock()
}

// ReserveAndWriteFast appends a log record immediately.
func ReserveAndWriteFast(data []byte) (endLSN uint64, startLSN uint64) {
	if System == nil {
		Init()
	}
	System.mu.Lock()
	defer System.mu.Unlock()
	start := System.lsn
	end := start + uint64(len(data))
	System.entries = append(System.entries, Entry{
		StartLSN: start,
		EndLSN:   end,
		Data:     append([]byte(nil), data...),
	})
	System.lsn = end
	System.writeRecord(start, data)
	return end, start
}

// ReserveAndOpen reserves space for a log record.
func ReserveAndOpen(length int) uint64 {
	if System == nil {
		Init()
	}
	System.mu.Lock()
	defer System.mu.Unlock()
	System.open = true
	System.openStart = System.lsn
	System.pending = make([]byte, 0, length)
	return System.openStart
}

// WriteLow appends to the open log record.
func WriteLow(data []byte) {
	if System == nil {
		Init()
	}
	System.mu.Lock()
	defer System.mu.Unlock()
	if !System.open {
		return
	}
	System.pending = append(System.pending, data...)
}

// Close finalizes the open log record.
func Close() uint64 {
	if System == nil {
		return 0
	}
	System.mu.Lock()
	defer System.mu.Unlock()
	if !System.open {
		return System.lsn
	}
	end := System.openStart + uint64(len(System.pending))
	System.entries = append(System.entries, Entry{
		StartLSN: System.openStart,
		EndLSN:   end,
		Data:     append([]byte(nil), System.pending...),
	})
	System.lsn = end
	System.writeRecord(System.openStart, System.pending)
	System.open = false
	System.pending = nil
	return end
}

// FlushUpTo advances the flushed lsn.
func FlushUpTo(lsn uint64) uint64 {
	if System == nil {
		return 0
	}
	System.mu.Lock()
	defer System.mu.Unlock()
	if lsn > System.lsn {
		lsn = System.lsn
	}
	if lsn > System.flushed {
		System.flushed = lsn
	}
	return System.flushed
}

// CurrentLSN returns the current lsn.
func CurrentLSN() uint64 {
	if System == nil {
		return 0
	}
	System.mu.Lock()
	defer System.mu.Unlock()
	return System.lsn
}

// Entries returns a snapshot of log entries.
func Entries() []Entry {
	if System == nil {
		return nil
	}
	System.mu.Lock()
	defer System.mu.Unlock()
	out := make([]Entry, len(System.entries))
	copy(out, System.entries)
	return out
}
