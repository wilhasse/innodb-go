package fsp

import (
	"errors"
	"fmt"

	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/mach"
	ibos "github.com/wilhasse/innodb-go/os"
	"github.com/wilhasse/innodb-go/ut"
)

// TablespaceFileSpec describes a single tablespace file.
type TablespaceFileSpec struct {
	Path                     string
	SizeBytes                uint64
	Autoextend               bool
	AutoextendIncrementBytes uint64
}

// SystemTablespaceSpec describes the system tablespace file layout.
type SystemTablespaceSpec struct {
	Files []TablespaceFileSpec
}

// OpenSystemTablespace creates or opens the system tablespace files and loads their headers.
func OpenSystemTablespace(spec SystemTablespaceSpec) error {
	if len(spec.Files) == 0 {
		return nil
	}
	for i, fileSpec := range spec.Files {
		if fileSpec.Autoextend && i != len(spec.Files)-1 {
			return errors.New("fsp: autoextend only allowed on last datafile")
		}
	}
	space := fil.SpaceGetByID(0)
	if space == nil {
		return errors.New("fsp: system tablespace not registered")
	}
	for _, node := range space.Nodes {
		if node.File != nil {
			_ = ibos.FileClose(node.File)
			node.File = nil
			node.Open = false
		}
	}
	space.Nodes = nil
	space.Size = 0
	space.File = nil

	opened := make([]ibos.File, 0, len(spec.Files))
	cleanup := func(err error) error {
		closeFiles(opened)
		for _, node := range space.Nodes {
			if node != nil {
				node.File = nil
				node.Open = false
			}
		}
		space.Nodes = nil
		space.Size = 0
		space.File = nil
		return err
	}

	var totalPages uint64
	createdFirst := false
	for i, fileSpec := range spec.Files {
		file, sizePages, created, err := openTablespaceFile(fileSpec)
		if err != nil {
			return cleanup(err)
		}
		opened = append(opened, file)
		node, err := fil.NodeCreate(fileSpec.Path, sizePages, 0, false)
		if err != nil {
			return cleanup(err)
		}
		node.File = file
		node.Open = true
		if i == 0 {
			space.File = file
			createdFirst = created
		}
		totalPages += sizePages
	}

	last := spec.Files[len(spec.Files)-1]
	space.Autoextend = last.Autoextend
	space.AutoextendInc = last.AutoextendIncrementBytes

	if space.File == nil {
		return cleanup(errors.New("fsp: system tablespace missing primary file"))
	}

	headerSize := uint32(totalPages)
	var headerPage []byte
	if createdFirst {
		headerPage = make([]byte, ut.UNIV_PAGE_SIZE)
		if err := initSystemHeaderPage(headerPage, 0, headerSize, 0); err != nil {
			return cleanup(err)
		}
		if err := fil.WritePage(space.File, 0, headerPage); err != nil {
			return cleanup(err)
		}
		currentFreeLimit = headerSize
	} else {
		page, err := fil.ReadPage(space.File, 0)
		if err != nil {
			return cleanup(err)
		}
		headerPage = page
		headerSize = GetSizeLow(page)
		space.Flags = HeaderGetFlags(page)
		freeLimit := readUint32(page, HeaderOffset+FreeLimitOffset)
		if headerSize == 0 {
			headerSize = uint32(totalPages)
			writeUint32(page, HeaderOffset+SizeOffset, headerSize)
			writeUint32(page, HeaderOffset+FreeLimitOffset, headerSize)
		}
		if freeLimit == 0 {
			currentFreeLimit = headerSize
		} else {
			currentFreeLimit = freeLimit
		}
	}

	if err := loadAllocFromHeader(0, headerPage); err != nil {
		return cleanup(err)
	}

	metas, err := readNodeMetadata(headerPage)
	if err != nil {
		return cleanup(err)
	}
	if len(metas) > 0 {
		if len(metas) != len(space.Nodes) {
			return cleanup(errors.New("fsp: node metadata count mismatch"))
		}
		totalPages = 0
		for i, meta := range metas {
			if meta.name != space.Nodes[i].Name {
				return cleanup(errors.New("fsp: node metadata name mismatch"))
			}
			space.Nodes[i].Size = meta.sizePages
			totalPages += meta.sizePages
			if space.Nodes[i].File != nil {
				_ = ensureFileSize(space.Nodes[i].File, meta.sizePages*ut.UNIV_PAGE_SIZE)
			}
		}
	}

	if headerSize == 0 {
		headerSize = uint32(totalPages)
	}
	if totalPages == 0 {
		totalPages = uint64(headerSize)
	}
	if headerSize > uint32(totalPages) && len(metas) == 0 && len(space.Nodes) > 0 {
		delta := headerSize - uint32(totalPages)
		lastNode := space.Nodes[len(space.Nodes)-1]
		lastNode.Size += uint64(delta)
		totalPages = uint64(headerSize)
		if lastNode.File != nil {
			_ = ensureFileSize(lastNode.File, uint64(lastNode.Size)*ut.UNIV_PAGE_SIZE)
		}
	} else if headerSize < uint32(totalPages) {
		headerSize = uint32(totalPages)
	}

	space.Size = totalPages
	if currentFreeLimit < headerSize {
		currentFreeLimit = headerSize
	}

	if err := persistSystemHeader(0, headerSize, currentFreeLimit); err != nil {
		return cleanup(err)
	}
	desiredExtents := extentCountForPages(uint32(space.Size))
	if desiredExtents > maxExtentsForPage() {
		return cleanup(errors.New("fsp: extent map exceeds header capacity"))
	}
	alloc := ensureExtentCount(0, desiredExtents)
	if err := persistExtentMap(0, alloc); err != nil {
		return cleanup(err)
	}
	if err := persistNodeMetadata(0, space.Nodes); err != nil {
		return cleanup(err)
	}

	return nil
}

// CloseSystemTablespace closes the system tablespace file handle.
func CloseSystemTablespace() error {
	space := fil.SpaceGetByID(0)
	if space != nil && space.File != nil {
		_ = persistSystemHeader(0, uint32(space.Size), currentFreeLimit)
		_ = persistExtentMap(0, ensureAlloc(0))
		_ = persistNodeMetadata(0, space.Nodes)
	}
	fil.SpaceCloseFile(0)
	return nil
}

func openTablespaceFile(spec TablespaceFileSpec) (ibos.File, uint64, bool, error) {
	if spec.Path == "" || spec.SizeBytes == 0 {
		return nil, 0, false, errors.New("fsp: missing datafile spec")
	}
	if spec.SizeBytes < ut.UNIV_PAGE_SIZE {
		return nil, 0, false, errors.New("fsp: datafile size too small")
	}
	if spec.SizeBytes%ut.UNIV_PAGE_SIZE != 0 {
		return nil, 0, false, fmt.Errorf("fsp: datafile size must align to %d bytes", ut.UNIV_PAGE_SIZE)
	}
	if err := ibos.FileCreateSubdirsIfNeeded(spec.Path); err != nil {
		return nil, 0, false, err
	}
	exists, err := ibos.FileExists(spec.Path)
	if err != nil {
		return nil, 0, false, err
	}
	createMode := ibos.FileOpen
	if !exists {
		createMode = ibos.FileCreate
	}
	file, err := ibos.FileCreateSimple(spec.Path, createMode, ibos.FileReadWrite)
	if err != nil {
		return nil, 0, false, err
	}
	fileSize, err := ibos.FileSize(file)
	if err != nil {
		_ = ibos.FileClose(file)
		return nil, 0, false, err
	}
	if fileSize%int64(ut.UNIV_PAGE_SIZE) != 0 {
		_ = ibos.FileClose(file)
		return nil, 0, false, errors.New("fsp: datafile size not aligned")
	}
	sizePages := uint64(spec.SizeBytes / ut.UNIV_PAGE_SIZE)
	filePages := uint64(fileSize / int64(ut.UNIV_PAGE_SIZE))
	if filePages > sizePages {
		sizePages = filePages
	}
	if err := ensureFileSize(file, sizePages*ut.UNIV_PAGE_SIZE); err != nil {
		_ = ibos.FileClose(file)
		return nil, 0, false, err
	}
	return file, sizePages, !exists, nil
}

func closeFiles(files []ibos.File) {
	for _, file := range files {
		if file == nil {
			continue
		}
		_ = ibos.FileClose(file)
	}
}

func ensureFileSize(file ibos.File, sizeBytes uint64) error {
	curSize, err := ibos.FileSize(file)
	if err != nil {
		return err
	}
	if curSize >= int64(sizeBytes) {
		return nil
	}
	if preallocateFilesEnabled() {
		return ibos.FilePreallocate(file, int64(sizeBytes))
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

func persistNodeMetadata(spaceID uint32, nodes []*fil.Node) error {
	space := fil.SpaceGetByID(spaceID)
	if space == nil || space.File == nil {
		return nil
	}
	page, err := fil.ReadPage(space.File, 0)
	if err != nil {
		page = make([]byte, ut.UNIV_PAGE_SIZE)
		if initErr := initSystemHeaderPage(page, spaceID, uint32(space.Size), space.Flags); initErr != nil {
			return initErr
		}
	}
	metas := make([]nodeMeta, 0, len(nodes))
	for _, node := range nodes {
		if node == nil {
			continue
		}
		metas = append(metas, nodeMeta{name: node.Name, sizePages: node.Size})
	}
	if err := writeNodeMetadata(page, metas); err != nil {
		return err
	}
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
