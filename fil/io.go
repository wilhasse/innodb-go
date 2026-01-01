package fil

import (
	"errors"
	"io"

	iblog "github.com/wilhasse/innodb-go/log"
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

// SpaceReadPage reads a page from the file attached to the tablespace.
func SpaceReadPage(spaceID, pageNo uint32) ([]byte, error) {
	buf := make([]byte, ut.UNIV_PAGE_SIZE)
	if err := SpaceReadPageInto(spaceID, pageNo, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// SpaceReadPageInto reads a page into buf for the attached tablespace file.
func SpaceReadPageInto(spaceID, pageNo uint32, buf []byte) error {
	if len(buf) < ut.UNIV_PAGE_SIZE {
		return errors.New("fil: page buffer too small")
	}
	clear(buf[:ut.UNIV_PAGE_SIZE])
	file := SpaceGetFile(spaceID)
	if file == nil {
		return nil
	}
	_, err := ibos.FileReadPage(file, pageNo, buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	iblog.RecvRecoverPage(spaceID, pageNo, buf)
	return nil
}

// SpaceWritePage writes a page to the file attached to the tablespace.
func SpaceWritePage(spaceID, pageNo uint32, data []byte) error {
	file := SpaceGetFile(spaceID)
	if file == nil {
		SpaceEnsureSize(spaceID, uint64(pageNo)+1)
		return nil
	}
	if err := WritePage(file, pageNo, data); err != nil {
		return err
	}
	SpaceEnsureSize(spaceID, uint64(pageNo)+1)
	return nil
}
