package log

import (
	ibos "github.com/wilhasse/innodb-go/os"
)

func (l *Log) lsnToOffset(lsn uint64) int64 {
	if l == nil {
		return int64(logHeaderSize)
	}
	if lsn < l.startLSN {
		return int64(logHeaderSize)
	}
	return int64(logHeaderSize) + int64(lsn-l.startLSN)
}

func (l *Log) writeRecord(startLSN uint64, data []byte) {
	if l == nil || l.file == nil || len(data) == 0 {
		return
	}
	offset := l.lsnToOffset(startLSN)
	if _, err := ibos.FileWriteAt(l.file, data, offset); err != nil {
		return
	}
	end := offset + int64(len(data))
	if end > 0 && uint64(end) > l.fileSize {
		l.fileSize = uint64(end)
	}
}
