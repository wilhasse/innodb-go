package log

import (
	"sync"
	"sync/atomic"

	ibos "github.com/wilhasse/innodb-go/os"
)

const defaultLogBufferSize = 8 << 20

func (l *Log) initBuffer(bufSize uint64) {
	if l == nil {
		return
	}
	if bufSize == 0 {
		bufSize = defaultLogBufferSize
	}
	if bufSize > uint64(^uint(0)>>1) {
		bufSize = defaultLogBufferSize
	}
	l.buf = make([]byte, int(bufSize))
	l.bufStartLSN = l.lsn
	l.flushCond = sync.NewCond(&l.mu)
	l.writerDone = make(chan struct{})
	go l.writerLoop()
}

func (l *Log) appendToBufferLocked(data []byte, startLSN uint64) {
	if l == nil || len(data) == 0 {
		return
	}
	if len(l.buf) == 0 {
		l.buf = make([]byte, defaultLogBufferSize)
	}
	if l.bufUsed == 0 {
		l.bufStartLSN = startLSN
	}
	offset := 0
	for offset < len(data) {
		for l.bufUsed == len(l.buf) {
			l.signalWriterLocked()
			l.flushCond.Wait()
		}
		if l.bufUsed == 0 {
			l.bufStartLSN = startLSN + uint64(offset)
		}
		avail := len(l.buf) - l.bufUsed
		chunk := len(data) - offset
		if chunk > avail {
			chunk = avail
		}
		copy(l.buf[l.bufUsed:], data[offset:offset+chunk])
		l.bufUsed += chunk
		offset += chunk
		if l.bufUsed == len(l.buf) {
			l.signalWriterLocked()
		}
	}
	l.signalWriterLocked()
}

func (l *Log) signalWriterLocked() {
	if l.flushCond != nil {
		l.flushCond.Broadcast()
	}
}

func (l *Log) shouldFlushLocked() bool {
	if l == nil || l.bufUsed == 0 {
		return false
	}
	if l.stopWriter {
		return true
	}
	if l.flushRequested > l.flushed {
		return true
	}
	return l.bufUsed == len(l.buf)
}

func (l *Log) writerLoop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for {
		for !l.stopWriter && !l.shouldFlushLocked() && !(l.flushRequested > l.flushed && l.bufUsed == 0) {
			l.flushCond.Wait()
		}
		if l.stopWriter && l.bufUsed == 0 {
			close(l.writerDone)
			return
		}
		if l.bufUsed == 0 && l.flushRequested > l.flushed {
			l.flushed = l.flushRequested
			l.flushRequested = l.flushed
			l.persistHeader()
			if l.file != nil {
				_ = ibos.FileFlush(l.file)
			}
			atomic.AddUint64(&NLogFlushes, 1)
			atomic.StoreUint64(&NPendingLogFlushes, 0)
			l.signalWriterLocked()
			continue
		}
		if !l.shouldFlushLocked() {
			continue
		}
		start := l.bufStartLSN
		data := make([]byte, l.bufUsed)
		copy(data, l.buf[:l.bufUsed])
		l.bufUsed = 0
		l.bufStartLSN = start + uint64(len(data))
		l.mu.Unlock()
		l.writeRecord(start, data)
		l.mu.Lock()
		end := start + uint64(len(data))
		if l.stopWriter || (l.flushRequested > l.flushed && end >= l.flushRequested) {
			l.flushed = end
			l.flushRequested = l.flushed
			l.persistHeader()
			if l.file != nil {
				_ = ibos.FileFlush(l.file)
			}
			atomic.AddUint64(&NLogFlushes, 1)
			atomic.StoreUint64(&NPendingLogFlushes, 0)
		}
		l.signalWriterLocked()
	}
}
