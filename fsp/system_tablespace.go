package fsp

import (
	"errors"
	"fmt"

	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/mach"
	ibos "github.com/wilhasse/innodb-go/os"
	"github.com/wilhasse/innodb-go/ut"
)

// SystemTablespaceSpec describes the system tablespace file layout.
type SystemTablespaceSpec struct {
	Path                     string
	SizeBytes                uint64
	Autoextend               bool
	AutoextendIncrementBytes uint64
}

// OpenSystemTablespace creates or opens the system tablespace file and loads its header.
func OpenSystemTablespace(spec SystemTablespaceSpec) error {
	if spec.Path == "" || spec.SizeBytes == 0 {
		return nil
	}
	if spec.SizeBytes < ut.UNIV_PAGE_SIZE {
		return errors.New("fsp: system tablespace size too small")
	}
	if spec.SizeBytes%ut.UNIV_PAGE_SIZE != 0 {
		return fmt.Errorf("fsp: system tablespace size must align to %d bytes", ut.UNIV_PAGE_SIZE)
	}
	space := fil.SpaceGetByID(0)
	if space == nil {
		return errors.New("fsp: system tablespace not registered")
	}
	if err := ibos.FileCreateSubdirsIfNeeded(spec.Path); err != nil {
		return err
	}
	exists, err := ibos.FileExists(spec.Path)
	if err != nil {
		return err
	}
	createMode := ibos.FileOpen
	if !exists {
		createMode = ibos.FileCreate
	}
	file, err := ibos.FileCreateSimple(spec.Path, createMode, ibos.FileReadWrite)
	if err != nil {
		return err
	}

	flags := uint32(0)
	sizePages := uint32(spec.SizeBytes / ut.UNIV_PAGE_SIZE)
	var headerPage []byte
	if !exists {
		headerPage = make([]byte, ut.UNIV_PAGE_SIZE)
		if err := initSystemHeaderPage(headerPage, 0, sizePages, flags); err != nil {
			_ = ibos.FileClose(file)
			return err
		}
		if err := fil.WritePage(file, 0, headerPage); err != nil {
			_ = ibos.FileClose(file)
			return err
		}
		if err := ensureFileSize(file, spec.SizeBytes); err != nil {
			_ = ibos.FileClose(file)
			return err
		}
	} else {
		fileSize, err := ibos.FileSize(file)
		if err != nil {
			_ = ibos.FileClose(file)
			return err
		}
		if fileSize < int64(ut.UNIV_PAGE_SIZE) {
			_ = ibos.FileClose(file)
			return errors.New("fsp: system tablespace file too small")
		}
		page, err := fil.ReadPage(file, 0)
		if err != nil {
			_ = ibos.FileClose(file)
			return err
		}
		headerPage = page
		headerSize := GetSizeLow(page)
		flags = HeaderGetFlags(page)
		freeLimit := readUint32(page, HeaderOffset+FreeLimitOffset)
		filePages := uint32(fileSize / int64(ut.UNIV_PAGE_SIZE))
		if headerSize == 0 {
			headerSize = filePages
			writeUint32(page, HeaderOffset+SizeOffset, headerSize)
			writeUint32(page, HeaderOffset+FreeLimitOffset, headerSize)
			if err := fil.WritePage(file, 0, page); err != nil {
				_ = ibos.FileClose(file)
				return err
			}
		}
		if filePages < headerSize {
			_ = ibos.FileClose(file)
			return errors.New("fsp: system tablespace header size exceeds file size")
		}
		sizePages = headerSize
		if freeLimit == 0 {
			currentFreeLimit = headerSize
		} else {
			currentFreeLimit = freeLimit
		}
	}

	if err := fil.SpaceSetFile(0, file); err != nil {
		_ = ibos.FileClose(file)
		return err
	}
	if err := loadAllocFromHeader(0, headerPage); err != nil {
		_ = ibos.FileClose(file)
		return err
	}
	fil.SpaceEnsureSize(0, uint64(sizePages))
	space.Flags = flags
	space.Autoextend = spec.Autoextend
	space.AutoextendInc = spec.AutoextendIncrementBytes
	return nil
}

// CloseSystemTablespace closes the system tablespace file handle.
func CloseSystemTablespace() error {
	space := fil.SpaceGetByID(0)
	if space != nil && space.File != nil {
		_ = persistSystemHeader(0, uint32(space.Size), currentFreeLimit)
	}
	fil.SpaceCloseFile(0)
	return nil
}

func ensureFileSize(file ibos.File, sizeBytes uint64) error {
	curSize, err := ibos.FileSize(file)
	if err != nil {
		return err
	}
	if curSize >= int64(sizeBytes) {
		return nil
	}
	_, err = ibos.FileWriteAt(file, []byte{0}, int64(sizeBytes)-1)
	return err
}

func initSystemHeaderPage(page []byte, spaceID uint32, sizePages uint32, flags uint32) error {
	clear(page)
	mach.WriteTo4(page[int(fil.PageSpaceOrChecksum):], spaceID)
	mach.WriteTo4(page[int(fil.PageOffset):], 0)
	mach.WriteTo2(page[int(fil.PageType):], uint32(fil.PageTypeFspHdr))
	mach.WriteTo4(page[int(fil.PageArchLogNoOrSpaceID):], spaceID)
	HeaderInit(page, spaceID, sizePages, flags)
	extentCount := extentCountForPages(sizePages)
	if extentCount > maxExtentsForPage() {
		return errors.New("fsp: extent map exceeds header capacity")
	}
	HeaderSetExtentCount(page, extentCount)
	for idx := uint32(0); idx < extentCount; idx++ {
		off := extentMapOffset(idx)
		clear(page[off : off+extentBitmapBytes])
	}
	if extentCount > 0 {
		setExtentBitInPage(page, 0, 0, true)
	}
	return nil
}

func persistSystemHeader(spaceID uint32, sizePages uint32, freeLimit uint32) error {
	space := fil.SpaceGetByID(spaceID)
	if space == nil || space.File == nil {
		return nil
	}
	page, err := fil.ReadPage(space.File, 0)
	if err != nil {
		page = make([]byte, ut.UNIV_PAGE_SIZE)
		if initErr := initSystemHeaderPage(page, spaceID, sizePages, space.Flags); initErr != nil {
			return initErr
		}
	}
	writeUint32(page, HeaderOffset+SizeOffset, sizePages)
	writeUint32(page, HeaderOffset+FreeLimitOffset, freeLimit)
	return fil.WritePage(space.File, 0, page)
}

func setExtentBitInPage(page []byte, extentIdx uint32, pageOff uint32, used bool) {
	if pageOff >= uint32(ExtentSize) {
		return
	}
	off := extentMapOffset(extentIdx)
	if off+extentBitmapBytes > len(page) {
		return
	}
	byteIdx := off + int(pageOff/8)
	mask := byte(1 << (pageOff % 8))
	if used {
		page[byteIdx] |= mask
		return
	}
	page[byteIdx] &^= mask
}
