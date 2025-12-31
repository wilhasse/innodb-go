package fil

import (
	"errors"

	ibos "github.com/wilhasse/innodb-go/os"
	"github.com/wilhasse/innodb-go/ut"
)

// ReadPage reads a 16KB page from a tablespace file.
func ReadPage(file ibos.File, pageNo uint32) ([]byte, error) {
	if file == nil {
		return nil, errors.New("fil: nil file")
	}
	buf := make([]byte, ut.UNIV_PAGE_SIZE)
	if _, err := ibos.FileReadPage(file, pageNo, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// WritePage writes a 16KB page to a tablespace file.
func WritePage(file ibos.File, pageNo uint32, data []byte) error {
	if file == nil {
		return errors.New("fil: nil file")
	}
	if len(data) < ut.UNIV_PAGE_SIZE {
		return errors.New("fil: page buffer too small")
	}
	_, err := ibos.FileWritePage(file, pageNo, data)
	return err
}
