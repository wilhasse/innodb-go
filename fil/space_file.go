package fil

import (
	"errors"

	ibos "github.com/wilhasse/innodb-go/os"
	"github.com/wilhasse/innodb-go/ut"
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
	if file == nil {
		return nil
	}
	if len(space.Nodes) == 0 {
		sizePages := uint64(0)
		if size, err := ibos.FileSize(file); err == nil && size > 0 {
			sizePages = uint64(size / int64(ut.UNIV_PAGE_SIZE))
		}
		node := &Node{
			Space: space,
			Name:  file.Name(),
			Open:  true,
			Size:  sizePages,
			File:  file,
		}
		space.Nodes = append(space.Nodes, node)
		if sizePages > 0 {
			space.Size = sizePages
		}
		return nil
	}
	node := space.Nodes[0]
	node.File = file
	node.Open = true
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
	if space.File == nil && len(space.Nodes) > 0 {
		return space.Nodes[0].File
	}
	return space.File
}

// SpaceCloseFile closes and clears the file handle for a tablespace.
func SpaceCloseFile(id uint32) {
	sys := ensureSystem()
	sys.mu.Lock()
	defer sys.mu.Unlock()
	space := sys.spacesByID[id]
	if space == nil {
		return
	}
	for _, node := range space.Nodes {
		if node.File == nil {
			continue
		}
		_ = ibos.FileClose(node.File)
		node.File = nil
		node.Open = false
	}
	space.File = nil
}
