package fil

import (
	"errors"

	ibos "github.com/wilhasse/innodb-go/os"
)

// ErrNoSpaceFile reports a missing tablespace file.
var ErrNoSpaceFile = errors.New("fil: no file for space")

// SpaceSetFile attaches a file handle to a tablespace.
func SpaceSetFile(id uint32, file ibos.File) error {
	sys := ensureSystem()
	sys.mu.Lock()
	defer sys.mu.Unlock()
	space := sys.spacesByID[id]
	if space == nil {
		return errors.New("fil: space not found")
	}
	space.File = file
	return nil
}

// SpaceGetFile returns the file handle for a tablespace.
func SpaceGetFile(id uint32) ibos.File {
	sys := ensureSystem()
	sys.mu.Lock()
	defer sys.mu.Unlock()
	space := sys.spacesByID[id]
	if space == nil {
		return nil
	}
	return space.File
}

// SpaceCloseFile closes and clears the file handle for a tablespace.
func SpaceCloseFile(id uint32) {
	sys := ensureSystem()
	sys.mu.Lock()
	defer sys.mu.Unlock()
	space := sys.spacesByID[id]
	if space == nil || space.File == nil {
		return
	}
	_ = ibos.FileClose(space.File)
	space.File = nil
}
