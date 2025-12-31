package ibuf

import (
	"sync"

	"github.com/wilhasse/innodb-go/dict"
	"github.com/wilhasse/innodb-go/ut"
)

// UseType controls which operations can be buffered.
type UseType int

const (
	UseNone UseType = iota
	UseInsert
	UseCount
)

// BitmapPageOffset mirrors FSP_IBUF_BITMAP_OFFSET.
const BitmapPageOffset uint32 = 1

// BufferEntry stores a buffered operation.
type BufferEntry struct {
	SpaceID uint32
	PageNo  uint32
	Data    []byte
}

type bufferKey struct {
	spaceID uint32
	pageNo  uint32
}

// Buffer stores buffered operations by page.
type Buffer struct {
	mu      sync.Mutex
	entries map[bufferKey][]BufferEntry
	count   uint64
}

// Global insert buffer state.
var (
	Use          UseType = UseInsert
	InsertBuffer *Buffer
	FlushCount   uint64
	MaxSpaceID   uint32
)

// InitAtDBStart initializes the insert buffer.
func InitAtDBStart() {
	InsertBuffer = &Buffer{entries: make(map[bufferKey][]BufferEntry)}
	FlushCount = 0
	MaxSpaceID = 0
}

// UpdateMaxTablespaceID updates MaxSpaceID based on buffered entries.
func UpdateMaxTablespaceID() {
	if InsertBuffer == nil {
		MaxSpaceID = 0
		return
	}
	InsertBuffer.mu.Lock()
	defer InsertBuffer.mu.Unlock()
	var max uint32
	for key := range InsertBuffer.entries {
		if key.spaceID > max {
			max = key.spaceID
		}
	}
	MaxSpaceID = max
}

// Insert records a buffered operation for a page.
func Insert(spaceID, pageNo uint32, data []byte) {
	if InsertBuffer == nil {
		InitAtDBStart()
	}
	InsertBuffer.mu.Lock()
	defer InsertBuffer.mu.Unlock()
	key := bufferKey{spaceID: spaceID, pageNo: pageNo}
	entry := BufferEntry{
		SpaceID: spaceID,
		PageNo:  pageNo,
		Data:    append([]byte(nil), data...),
	}
	InsertBuffer.entries[key] = append(InsertBuffer.entries[key], entry)
	InsertBuffer.count++
	if spaceID > MaxSpaceID {
		MaxSpaceID = spaceID
	}
}

// Get returns buffered entries for a page.
func Get(spaceID, pageNo uint32) []BufferEntry {
	if InsertBuffer == nil {
		return nil
	}
	InsertBuffer.mu.Lock()
	defer InsertBuffer.mu.Unlock()
	key := bufferKey{spaceID: spaceID, pageNo: pageNo}
	entries := InsertBuffer.entries[key]
	out := make([]BufferEntry, len(entries))
	copy(out, entries)
	return out
}

// Delete removes buffered entries for a page.
func Delete(spaceID, pageNo uint32) {
	if InsertBuffer == nil {
		return
	}
	InsertBuffer.mu.Lock()
	defer InsertBuffer.mu.Unlock()
	key := bufferKey{spaceID: spaceID, pageNo: pageNo}
	removed := len(InsertBuffer.entries[key])
	delete(InsertBuffer.entries, key)
	if removed > 0 {
		InsertBuffer.count -= uint64(removed)
	}
}

// Count returns the total buffered entry count.
func Count() uint64 {
	if InsertBuffer == nil {
		return 0
	}
	InsertBuffer.mu.Lock()
	defer InsertBuffer.mu.Unlock()
	return InsertBuffer.count
}

// ShouldTry reports whether insert buffering is recommended.
func ShouldTry(index *dict.Index, ignoreSecUnique bool) bool {
	if Use == UseNone || index == nil {
		return false
	}
	if index.Clustered {
		return false
	}
	if index.Unique && !ignoreSecUnique {
		return false
	}
	FlushCount++
	return true
}

// BitmapPage checks if a page number is an ibuf bitmap page.
func BitmapPage(zipSize uint32, pageNo uint32) bool {
	if zipSize == 0 {
		return pageNo%uint32(ut.UnivPageSize) == BitmapPageOffset
	}
	return pageNo%zipSize == BitmapPageOffset
}
