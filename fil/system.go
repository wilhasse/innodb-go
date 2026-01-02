package fil

import (
	"errors"
	"sync"

	ibos "github.com/wilhasse/innodb-go/os"
)

// NLogFlushes tracks the number of log flushes.
var NLogFlushes uint64

// NPendingLogFlushes tracks pending log flushes.
var NPendingLogFlushes uint64

// NPendingTablespaceFlushes tracks pending tablespace flushes.
var NPendingTablespaceFlushes uint64

// Node represents a file node in a tablespace chain.
type Node struct {
	Space          *Space
	Name           string
	Open           bool
	IsRaw          bool
	Size           uint64
	File           ibos.File
	PendingIO      uint64
	PendingFlushes uint64
	ModCounter     int64
	FlushCounter   int64
}

// Space represents a tablespace or log space.
type Space struct {
	Name           string
	ID             uint32
	Version        int64
	Mark           bool
	StopIOs        bool
	IsBeingDeleted bool
	Purpose        uint32
	Size           uint64
	Flags          uint32
	ZipSize        uint32
	Autoextend     bool
	AutoextendInc  uint64
	Nodes          []*Node
	File           ibos.File
}

// System holds the tablespace cache.
type System struct {
	mu           sync.Mutex
	spacesByID   map[uint32]*Space
	spacesByName map[string]*Space
}

var system *System

func newSystem() *System {
	return &System{
		spacesByID:   map[uint32]*Space{},
		spacesByName: map[string]*Space{},
	}
}

func ensureSystem() *System {
	if system == nil {
		system = newSystem()
	}
	return system
}

// VarInit resets the fil system and counters.
func VarInit() {
	system = newSystem()
	NLogFlushes = 0
	NPendingLogFlushes = 0
	NPendingTablespaceFlushes = 0
	externReset()
	resetDoublewriteState()
}

// SpaceCreate registers a tablespace or log space.
func SpaceCreate(name string, id uint32, zipSize uint32, purpose uint32) bool {
	sys := ensureSystem()
	sys.mu.Lock()
	defer sys.mu.Unlock()

	if _, ok := sys.spacesByID[id]; ok {
		return false
	}
	if _, ok := sys.spacesByName[name]; ok {
		return false
	}

	space := &Space{
		Name:    name,
		ID:      id,
		Purpose: purpose,
		ZipSize: zipSize,
	}

	sys.spacesByID[id] = space
	sys.spacesByName[name] = space
	return true
}

// SpaceRename updates the name for a tablespace.
func SpaceRename(id uint32, newName string) error {
	if newName == "" {
		return errors.New("fil: empty space name")
	}
	sys := ensureSystem()
	sys.mu.Lock()
	defer sys.mu.Unlock()
	space := sys.spacesByID[id]
	if space == nil {
		return errors.New("fil: space not found")
	}
	if _, exists := sys.spacesByName[newName]; exists {
		return errors.New("fil: space name exists")
	}
	delete(sys.spacesByName, space.Name)
	space.Name = newName
	sys.spacesByName[newName] = space
	return nil
}

// SpaceDrop removes a space from the cache.
func SpaceDrop(id uint32) {
	sys := ensureSystem()
	sys.mu.Lock()
	defer sys.mu.Unlock()

	space := sys.spacesByID[id]
	if space == nil {
		return
	}
	if space.File != nil {
		_ = ibos.FileClose(space.File)
		space.File = nil
	}
	for _, node := range space.Nodes {
		if node.File == nil {
			continue
		}
		_ = ibos.FileClose(node.File)
		node.File = nil
		node.Open = false
	}
	delete(sys.spacesByID, id)
	delete(sys.spacesByName, space.Name)
}

// SpaceGetByID returns a space by id.
func SpaceGetByID(id uint32) *Space {
	sys := ensureSystem()
	sys.mu.Lock()
	defer sys.mu.Unlock()

	return sys.spacesByID[id]
}

// SpaceGetByName returns a space by name.
func SpaceGetByName(name string) *Space {
	sys := ensureSystem()
	sys.mu.Lock()
	defer sys.mu.Unlock()

	return sys.spacesByName[name]
}

// SpaceGetSize returns a space size in pages.
func SpaceGetSize(id uint32) uint64 {
	space := SpaceGetByID(id)
	if space == nil {
		return 0
	}
	return space.Size
}

// SpaceEnsureSize grows cached space size if needed.
func SpaceEnsureSize(id uint32, size uint64) {
	sys := ensureSystem()
	sys.mu.Lock()
	defer sys.mu.Unlock()

	space := sys.spacesByID[id]
	if space == nil {
		return
	}
	if size > space.Size {
		delta := size - space.Size
		space.Size = size
		if len(space.Nodes) > 0 {
			space.Nodes[len(space.Nodes)-1].Size += delta
		}
	}
}

// SpaceGetType returns the space purpose.
func SpaceGetType(id uint32) uint32 {
	space := SpaceGetByID(id)
	if space == nil {
		return 0
	}
	return space.Purpose
}

// SpaceGetVersion returns the space version or -1.
func SpaceGetVersion(id uint32) int64 {
	space := SpaceGetByID(id)
	if space == nil {
		return -1
	}
	return space.Version
}

// TablespaceDeletedOrBeingDeletedInMem reports missing or deleted spaces.
func TablespaceDeletedOrBeingDeletedInMem(id uint32, version int64) bool {
	space := SpaceGetByID(id)
	if space == nil {
		return true
	}
	if space.IsBeingDeleted {
		return true
	}
	if version != -1 && space.Version != version {
		return true
	}
	return false
}

// TablespaceExistsInMem reports whether a space is cached.
func TablespaceExistsInMem(id uint32) bool {
	space := SpaceGetByID(id)
	return space != nil && !space.IsBeingDeleted
}

// NodeCreate appends a file node to a space.
func NodeCreate(name string, size uint64, id uint32, isRaw bool) (*Node, error) {
	sys := ensureSystem()
	sys.mu.Lock()
	defer sys.mu.Unlock()

	space := sys.spacesByID[id]
	if space == nil {
		return nil, errors.New("fil: space not found")
	}

	node := &Node{
		Space: space,
		Name:  name,
		Size:  size,
		IsRaw: isRaw,
	}
	space.Nodes = append(space.Nodes, node)
	if size > 0 {
		space.Size += size
	}
	return node, nil
}
