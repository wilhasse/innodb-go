package os

import (
	"errors"

	"github.com/wilhasse/innodb-go/ut"
)

// FileReadPage reads a full page into buf.
func FileReadPage(file File, pageNo uint32, buf []byte) (int, error) {
	if len(buf) < ut.UNIV_PAGE_SIZE {
		return 0, errors.New("os: buffer too small")
	}
	offset := int64(pageNo) * int64(ut.UNIV_PAGE_SIZE)
	return FileReadAt(file, buf[:ut.UNIV_PAGE_SIZE], offset)
}

// FileWritePage writes a full page from data.
func FileWritePage(file File, pageNo uint32, data []byte) (int, error) {
	if len(data) < ut.UNIV_PAGE_SIZE {
		return 0, errors.New("os: buffer too small")
	}
	offset := int64(pageNo) * int64(ut.UNIV_PAGE_SIZE)
	return FileWriteAt(file, data[:ut.UNIV_PAGE_SIZE], offset)
}
