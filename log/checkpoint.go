package log

import (
	ibos "github.com/wilhasse/innodb-go/os"
)

// Checkpoint persists the current checkpoint LSN.
func Checkpoint() uint64 {
	if System == nil {
		return 0
	}
	System.mu.Lock()
	defer System.mu.Unlock()
	lsn := System.flushed
	if lsn > System.lsn {
		lsn = System.lsn
	}
	System.checkpoint = lsn
	System.persistHeader()
	if System.file != nil {
		_ = ibos.FileFlush(System.file)
	}
	return System.checkpoint
}

// CheckpointLSN returns the current checkpoint LSN.
func CheckpointLSN() uint64 {
	if System == nil {
		return 0
	}
	System.mu.Lock()
	defer System.mu.Unlock()
	return System.checkpoint
}

// Shutdown flushes and closes the log file.
func Shutdown() {
	if System == nil {
		return
	}
	FlushUpTo(CurrentLSN())
	Checkpoint()
	System.mu.Lock()
	System.stopWriter = true
	System.signalWriterLocked()
	done := System.writerDone
	System.mu.Unlock()
	if done != nil {
		<-done
	}
	System.mu.Lock()
	file := System.file
	System.file = nil
	System.mu.Unlock()
	if file != nil {
		_ = ibos.FileFlush(file)
		_ = ibos.FileClose(file)
	}
	System = nil
	RecvSysVarInit()
}
