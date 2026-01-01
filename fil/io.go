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
	if err := verifyPageChecksum(buf); err != nil {
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
	applyPageChecksum(data)
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
	space := SpaceGetByID(spaceID)
	if space == nil {
		return nil
	}
	node, localPage := nodeForPage(space, pageNo)
	if node == nil || node.File == nil {
		if space.File == nil {
			return nil
		}
		_, err := ibos.FileReadPage(space.File, pageNo, buf)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if err == nil {
			if err := verifyPageChecksum(buf); err != nil {
				return err
			}
		}
		iblog.RecvRecoverPage(spaceID, pageNo, buf)
		return nil
	}
	_, err := ibos.FileReadPage(node.File, localPage, buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if err == nil {
		if err := verifyPageChecksum(buf); err != nil {
			return err
		}
	}
	iblog.RecvRecoverPage(spaceID, pageNo, buf)
	return nil
}

// SpaceWritePage writes a page to the file attached to the tablespace.
func SpaceWritePage(spaceID, pageNo uint32, data []byte) error {
	space := SpaceGetByID(spaceID)
	if space == nil {
		return nil
	}
	node, localPage := nodeForPage(space, pageNo)
	if node == nil || node.File == nil {
		if space.File == nil {
			SpaceEnsureSize(spaceID, uint64(pageNo)+1)
			return nil
		}
		if space.Purpose != SpaceLog {
			if err := DoublewriteWrite(spaceID, pageNo, data); err != nil {
				return err
			}
		}
		if err := WritePage(space.File, pageNo, data); err != nil {
			return err
		}
		SpaceEnsureSize(spaceID, uint64(pageNo)+1)
		return nil
	}
	if space.Purpose != SpaceLog {
		if err := DoublewriteWrite(spaceID, pageNo, data); err != nil {
			return err
		}
	}
	if err := WritePage(node.File, localPage, data); err != nil {
		return err
	}
	SpaceEnsureSize(spaceID, uint64(pageNo)+1)
	return nil
}

func nodeForPage(space *Space, pageNo uint32) (*Node, uint32) {
	if space == nil || len(space.Nodes) == 0 {
		return nil, 0
	}
	var base uint64
	for _, node := range space.Nodes {
		if node == nil || node.Size == 0 {
			continue
		}
		end := base + node.Size
		if uint64(pageNo) < end {
			return node, uint32(uint64(pageNo) - base)
		}
		base = end
	}
	return nil, 0
}
