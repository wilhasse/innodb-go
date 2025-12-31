package os

import (
	"errors"
	"io"
	"path/filepath"
	stdos "os"
	"sync/atomic"
)

const (
	FileOpen       = 51
	FileCreate     = 52
	FileOverwrite  = 53
	FileCreatePath = 55
)

const (
	FileReadOnly  = 333
	FileReadWrite = 444
)

// DefaultFilePerm matches the default umask of 0660.
const DefaultFilePerm FileMode = 0o660

// File IO counters.
var (
	NFileReads  uint64
	NFileWrites uint64
	NFileSyncs  uint64
)

// FileCreateSimple opens or creates a file based on the provided mode.
func FileCreateSimple(name string, createMode int, accessMode int) (File, error) {
	flags := 0
	switch accessMode {
	case FileReadOnly:
		flags = stdos.O_RDONLY
	case FileReadWrite:
		flags = stdos.O_RDWR
	default:
		return nil, errors.New("os: invalid access mode")
	}

	switch createMode {
	case FileOpen:
		// no extra flags
	case FileCreate:
		flags |= stdos.O_CREATE | stdos.O_EXCL
	case FileOverwrite:
		flags |= stdos.O_CREATE | stdos.O_TRUNC
	case FileCreatePath:
		if err := FileCreateSubdirsIfNeeded(name); err != nil {
			return nil, err
		}
		flags |= stdos.O_CREATE | stdos.O_EXCL
	default:
		return nil, errors.New("os: invalid create mode")
	}
	return OpenFile(name, flags, DefaultFilePerm)
}

// FileCreateSimpleNoErrorHandling is a convenience wrapper around FileCreateSimple.
func FileCreateSimpleNoErrorHandling(name string, createMode int, accessMode int) File {
	file, _ := FileCreateSimple(name, createMode, accessMode)
	return file
}

// FileCreateDirectory creates a directory.
func FileCreateDirectory(path string, failIfExists bool) error {
	if failIfExists {
		if _, err := Stat(path); err == nil {
			return errors.New("os: path exists")
		}
	}
	return MkdirAll(path, 0o770)
}

// FileCreateSubdirsIfNeeded creates parent directories for a path.
func FileCreateSubdirsIfNeeded(name string) error {
	dir := filepath.Dir(name)
	if dir == "." || dir == "/" || dir == "" {
		return nil
	}
	return MkdirAll(dir, 0o770)
}

// FileReadAt reads fully from the file at the given offset.
func FileReadAt(file File, buf []byte, offset int64) (int, error) {
	if file == nil {
		return 0, errors.New("os: nil file")
	}
	read := 0
	for read < len(buf) {
		n, err := file.ReadAt(buf[read:], offset+int64(read))
		if n > 0 {
			read += n
		}
		if err != nil {
			if err == io.EOF && read == len(buf) {
				break
			}
			return read, err
		}
	}
	atomic.AddUint64(&NFileReads, 1)
	return read, nil
}

// FileWriteAt writes fully to the file at the given offset.
func FileWriteAt(file File, buf []byte, offset int64) (int, error) {
	if file == nil {
		return 0, errors.New("os: nil file")
	}
	written := 0
	for written < len(buf) {
		n, err := file.WriteAt(buf[written:], offset+int64(written))
		if n > 0 {
			written += n
		}
		if err != nil {
			return written, err
		}
	}
	atomic.AddUint64(&NFileWrites, 1)
	return written, nil
}

// FileFlush flushes pending writes to storage.
func FileFlush(file File) error {
	if file == nil {
		return errors.New("os: nil file")
	}
	if err := file.Sync(); err != nil {
		return err
	}
	atomic.AddUint64(&NFileSyncs, 1)
	return nil
}

// FileClose closes a file handle.
func FileClose(file File) error {
	if file == nil {
		return nil
	}
	return file.Close()
}

// FileSize returns the size of a file by name.
func FileSize(file File) (int64, error) {
	if file == nil {
		return 0, errors.New("os: nil file")
	}
	info, err := Stat(file.Name())
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// FileExists reports whether a file exists.
func FileExists(name string) (bool, error) {
	_, err := Stat(name)
	if err == nil {
		return true, nil
	}
	if stdos.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// FileDelete removes a file.
func FileDelete(name string) error {
	return Remove(name)
}

// FileRename renames a file.
func FileRename(oldpath, newpath string) error {
	return Rename(oldpath, newpath)
}
