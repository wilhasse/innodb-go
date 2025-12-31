package buf

import (
	"errors"
	"math/bits"
	"sync"
)

// BuddyBlock represents a chunk allocated from the buddy allocator.
type BuddyBlock struct {
	allocator *BuddyAllocator
	offset    int
	size      int
	level     int
	buf       []byte
}

// Bytes returns the block storage.
func (b *BuddyBlock) Bytes() []byte {
	if b == nil {
		return nil
	}
	return b.buf
}

// Size returns the block size in bytes.
func (b *BuddyBlock) Size() int {
	if b == nil {
		return 0
	}
	return b.size
}

// BuddyAllocator is a binary buddy allocator for compressed pages.
type BuddyAllocator struct {
	mu        sync.Mutex
	arena     []byte
	totalSize int
	levels    int
	free      []map[int]struct{}
}

// NewBuddyAllocator allocates a buddy allocator backed by a single arena.
func NewBuddyAllocator(totalSize int) (*BuddyAllocator, error) {
	if totalSize <= 0 {
		totalSize = BufBuddyHigh
	}
	if totalSize < BufBuddyLow {
		return nil, errors.New("buddy allocator: total size below minimum")
	}
	totalSize = nextPow2(totalSize)
	if totalSize > BufBuddyHigh {
		return nil, errors.New("buddy allocator: total size exceeds page size")
	}

	levels := 0
	for size := BufBuddyLow; size < totalSize; size <<= 1 {
		levels++
	}
	levels++

	BufBuddyVarInit()
	alloc := &BuddyAllocator{
		arena:     make([]byte, totalSize),
		totalSize: totalSize,
		levels:    levels,
		free:      make([]map[int]struct{}, levels),
	}
	for i := range alloc.free {
		alloc.free[i] = make(map[int]struct{})
	}
	alloc.free[levels-1][0] = struct{}{}
	return alloc, nil
}

// Alloc reserves a block of at least the requested size.
func (b *BuddyAllocator) Alloc(size int) (*BuddyBlock, bool) {
	if b == nil {
		return nil, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	level, blockSize, ok := b.levelForSize(size)
	if !ok {
		return nil, false
	}

	searchLevel := level
	for searchLevel < b.levels && len(b.free[searchLevel]) == 0 {
		searchLevel++
	}
	if searchLevel >= b.levels {
		return nil, false
	}

	offset, ok := b.popFree(searchLevel)
	if !ok {
		return nil, false
	}

	for searchLevel > level {
		searchLevel--
		splitSize := b.blockSize(searchLevel)
		buddyOffset := offset + splitSize
		b.free[searchLevel][buddyOffset] = struct{}{}
	}

	b.updateStats(level, true)
	block := &BuddyBlock{
		allocator: b,
		offset:    offset,
		size:      blockSize,
		level:     level,
		buf:       b.arena[offset : offset+blockSize],
	}
	return block, true
}

// Free releases a previously allocated block.
func (b *BuddyAllocator) Free(block *BuddyBlock) error {
	if b == nil || block == nil {
		return errors.New("buddy allocator: nil block")
	}
	if block.allocator != b {
		return errors.New("buddy allocator: block does not belong to allocator")
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	level := block.level
	offset := block.offset
	if offset < 0 || offset+block.size > b.totalSize {
		return errors.New("buddy allocator: invalid block offset")
	}

	for level < b.levels-1 {
		buddyOffset := offset ^ b.blockSize(level)
		if _, ok := b.free[level][buddyOffset]; !ok {
			break
		}
		delete(b.free[level], buddyOffset)
		if buddyOffset < offset {
			offset = buddyOffset
		}
		level++
	}

	b.free[level][offset] = struct{}{}
	b.updateStats(block.level, false)
	return nil
}

func (b *BuddyAllocator) updateStats(level int, alloc bool) {
	if level < 0 || level >= len(BufBuddyStat) {
		return
	}
	if alloc {
		BufBuddyStat[level].Used++
		return
	}
	if BufBuddyStat[level].Used > 0 {
		BufBuddyStat[level].Used--
	}
}

func (b *BuddyAllocator) levelForSize(size int) (int, int, bool) {
	if size <= 0 {
		return 0, 0, false
	}
	if size < BufBuddyLow {
		size = BufBuddyLow
	}
	if size > b.totalSize {
		return 0, 0, false
	}
	blockSize := nextPow2(size)
	if blockSize > b.totalSize {
		return 0, 0, false
	}
	level := 0
	for s := BufBuddyLow; s < blockSize; s <<= 1 {
		level++
	}
	return level, blockSize, true
}

func (b *BuddyAllocator) blockSize(level int) int {
	return BufBuddyLow << level
}

func (b *BuddyAllocator) popFree(level int) (int, bool) {
	for offset := range b.free[level] {
		delete(b.free[level], offset)
		return offset, true
	}
	return 0, false
}

func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}
