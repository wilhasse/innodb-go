package log

import (
	"errors"

	ibos "github.com/wilhasse/innodb-go/os"
)

// RecvScanLogFile reads log bytes and stores parsed records into the recv hash.
func RecvScanLogFile(file ibos.File, startLSN, endLSN uint64) (uint64, uint64, error) {
	if file == nil {
		return 0, 0, errors.New("log: nil file")
	}
	if endLSN < startLSN {
		endLSN = startLSN
	}
	length := int64(endLSN - startLSN)
	if length == 0 {
		return startLSN, startLSN, nil
	}
	buf := make([]byte, length)
	offset := int64(logHeaderSize) + int64(startLSN)
	if System != nil {
		offset = System.lsnToOffset(startLSN)
	}
	if _, err := ibos.FileReadAt(file, buf, offset); err != nil {
		return 0, 0, err
	}
	var contiguous uint64
	var scanned uint64
	RecvScanLogRecs(true, buf, startLSN, &contiguous, &scanned)
	return contiguous, scanned, nil
}
