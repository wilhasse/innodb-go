package fil

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"

	ibos "github.com/wilhasse/innodb-go/os"
	"github.com/wilhasse/innodb-go/ut"
)

const (
	doublewriteFileName   = "ib_doublewrite"
	doublewriteEntryBytes = 8 + ut.UNIV_PAGE_SIZE
)

var (
	doublewriteMu         sync.Mutex
	doublewriteFile       ibos.File
	doublewritePath       string
	doublewritePos        int64
	doublewriteEnabled    bool
	doublewriteRecovering bool
	doublewritePages      uint64
	doublewriteWrites     uint64
)

// SetDoublewriteEnabled toggles the doublewrite buffer usage.
func SetDoublewriteEnabled(enabled bool) {
	doublewriteMu.Lock()
	defer doublewriteMu.Unlock()
	doublewriteEnabled = enabled
}

// DoublewriteEnabled reports whether doublewrite is active.
func DoublewriteEnabled() bool {
	doublewriteMu.Lock()
	defer doublewriteMu.Unlock()
	return doublewriteEnabled && doublewriteFile != nil
}

// DoublewriteStats returns the number of pages and writes issued to the buffer.
func DoublewriteStats() (uint64, uint64) {
	return atomic.LoadUint64(&doublewritePages), atomic.LoadUint64(&doublewriteWrites)
}

// DoublewriteInit opens the doublewrite buffer file in the provided directory.
func DoublewriteInit(dir string) error {
	doublewriteMu.Lock()
	defer doublewriteMu.Unlock()
	if !doublewriteEnabled {
		return nil
	}
	if doublewriteFile != nil {
		return nil
	}
	if dir == "" {
		dir = "."
	}
	path := filepath.Join(dir, doublewriteFileName)
	doublewritePath = path
	exists, err := ibos.FileExists(path)
	if err != nil {
		return err
	}
	var file ibos.File
	if exists {
		file, err = ibos.FileCreateSimple(path, ibos.FileOpen, ibos.FileReadWrite)
	} else {
		if err := ibos.FileCreateSubdirsIfNeeded(path); err != nil {
			return err
		}
		file, err = ibos.FileCreateSimple(path, ibos.FileCreatePath, ibos.FileReadWrite)
	}
	if err != nil {
		return err
	}
	doublewriteFile = file
	if size, err := ibos.FileSize(file); err == nil && size > 0 {
		doublewritePos = size
	} else {
		doublewritePos = 0
	}
	return nil
}

// DoublewriteShutdown closes the doublewrite file handle.
func DoublewriteShutdown() {
	doublewriteMu.Lock()
	file := doublewriteFile
	doublewriteFile = nil
	doublewritePath = ""
	doublewritePos = 0
	doublewriteRecovering = false
	doublewriteMu.Unlock()
	if file != nil {
		_ = ibos.FileClose(file)
	}
}

// DoublewriteWrite appends a page copy to the doublewrite buffer.
func DoublewriteWrite(spaceID, pageNo uint32, data []byte) error {
	doublewriteMu.Lock()
	if !doublewriteEnabled || doublewriteFile == nil || doublewriteRecovering {
		doublewriteMu.Unlock()
		return nil
	}
	if len(data) < ut.UNIV_PAGE_SIZE {
		doublewriteMu.Unlock()
		return errors.New("fil: doublewrite page buffer too small")
	}
	offset := doublewritePos
	doublewritePos += doublewriteEntryBytes
	file := doublewriteFile
	doublewriteMu.Unlock()

	entry := make([]byte, doublewriteEntryBytes)
	binary.BigEndian.PutUint32(entry[0:], spaceID)
	binary.BigEndian.PutUint32(entry[4:], pageNo)
	copy(entry[8:], data[:ut.UNIV_PAGE_SIZE])
	if _, err := ibos.FileWriteAt(file, entry, offset); err != nil {
		return err
	}
	if err := ibos.FileFlush(file); err != nil {
		return err
	}
	atomic.AddUint64(&doublewritePages, 1)
	atomic.AddUint64(&doublewriteWrites, 1)
	return nil
}

// DoublewriteRecover replays doublewrite buffer entries into their tablespaces.
func DoublewriteRecover() error {
	doublewriteMu.Lock()
	if !doublewriteEnabled || doublewriteFile == nil {
		doublewriteMu.Unlock()
		return nil
	}
	file := doublewriteFile
	path := doublewritePath
	size, err := ibos.FileSize(file)
	if err != nil {
		doublewriteMu.Unlock()
		return err
	}
	if size == 0 {
		doublewriteMu.Unlock()
		return nil
	}
	doublewriteRecovering = true
	doublewriteMu.Unlock()

	entry := make([]byte, doublewriteEntryBytes)
	for offset := int64(0); offset+doublewriteEntryBytes <= size; offset += doublewriteEntryBytes {
		if _, err := ibos.FileReadAt(file, entry, offset); err != nil {
			doublewriteMu.Lock()
			doublewriteRecovering = false
			doublewriteMu.Unlock()
			return err
		}
		spaceID := binary.BigEndian.Uint32(entry[0:])
		pageNo := binary.BigEndian.Uint32(entry[4:])
		_ = SpaceWritePage(spaceID, pageNo, entry[8:])
	}

	doublewriteMu.Lock()
	doublewriteRecovering = false
	_ = ibos.FileClose(doublewriteFile)
	doublewriteFile = nil
	doublewritePos = 0
	if path != "" {
		file, err := ibos.FileCreateSimple(path, ibos.FileOverwrite, ibos.FileReadWrite)
		if err != nil {
			doublewriteMu.Unlock()
			return err
		}
		doublewriteFile = file
	}
	doublewriteMu.Unlock()
	return nil
}

func resetDoublewriteState() {
	DoublewriteShutdown()
	doublewriteMu.Lock()
	doublewriteEnabled = false
	doublewriteRecovering = false
	doublewriteMu.Unlock()
	atomic.StoreUint64(&doublewritePages, 0)
	atomic.StoreUint64(&doublewriteWrites, 0)
}
