package mem

import (
	"fmt"

	"github.com/wilhasse/innodb-go/ut"
)

const (
	// HeapDynamic allocates heap blocks from the Go heap.
	HeapDynamic = iota
	// HeapBuffer marks heaps intended to grow into buffer-backed blocks.
	HeapBuffer
	// HeapBtrSearch is the optional flag used with buffer-backed heaps.
	HeapBtrSearch
)

const (
	BlockStartSize = 64
	MaxAllocInBuf  = int(ut.UNIV_PAGE_SIZE) - 200
)

// BlockStandardSize is the default block size used once heaps grow.
var BlockStandardSize = func() int {
	if ut.UNIV_PAGE_SIZE >= 16384 {
		return 8000
	}
	return MaxAllocInBuf
}()

// DefaultHeapPool backs standard heap blocks with sync.Pool.
var DefaultHeapPool = NewBufferPool(BlockStandardSize)

// Heap manages a stack-like allocation arena backed by blocks.
type Heap struct {
	blocks      []*heapBlock
	allocations []int
	heapType    int
	totalSize   int
	pool        *BufferPool
}

type heapBlock struct {
	buf  []byte
	used int
}

// HeapCreate creates a heap with a dynamic allocation strategy.
func HeapCreate(size int) *Heap {
	return NewHeapWithPool(size, HeapDynamic, DefaultHeapPool)
}

// HeapCreateInBuffer creates a heap intended to use buffer-backed blocks.
func HeapCreateInBuffer(size int) *Heap {
	return NewHeapWithPool(size, HeapBuffer, DefaultHeapPool)
}

// HeapCreateInBtrSearch creates a buffer heap with the BTR search flag.
func HeapCreateInBtrSearch(size int) *Heap {
	return NewHeapWithPool(size, HeapBuffer|HeapBtrSearch, DefaultHeapPool)
}

// NewHeapWithPool creates a heap with an optional block pool.
func NewHeapWithPool(size int, heapType int, pool *BufferPool) *Heap {
	if size <= 0 {
		size = BlockStartSize
	}
	h := &Heap{heapType: heapType, pool: pool}
	h.addBlock(size)
	return h
}

// Alloc reserves n bytes from the heap.
func (h *Heap) Alloc(size int) []byte {
	if h == nil || size <= 0 {
		return nil
	}
	block := h.blocks[len(h.blocks)-1]
	if size > len(block.buf)-block.used {
		block = h.addBlock(h.nextBlockSize(size))
	}
	start := block.used
	block.used += size
	h.allocations = append(h.allocations, size)
	return block.buf[start:block.used]
}

// AllocZero reserves n bytes and zeroes the result.
func (h *Heap) AllocZero(size int) []byte {
	buf := h.Alloc(size)
	clear(buf)
	return buf
}

// Dup copies data into the heap.
func (h *Heap) Dup(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	buf := h.Alloc(len(data))
	copy(buf, data)
	return buf
}

// StrDup copies a string into the heap with a trailing NUL byte.
func (h *Heap) StrDup(s string) []byte {
	buf := h.Alloc(len(s) + 1)
	copy(buf, s)
	buf[len(s)] = 0
	return buf
}

// StrDupl copies at most n bytes from s into the heap with a trailing NUL.
func (h *Heap) StrDupl(s string, n int) []byte {
	if n < 0 {
		n = 0
	}
	if n > len(s) {
		n = len(s)
	}
	buf := h.Alloc(n + 1)
	copy(buf, s[:n])
	buf[n] = 0
	return buf
}

// StrCat concatenates two strings into the heap with a trailing NUL.
func (h *Heap) StrCat(a, b string) []byte {
	buf := h.Alloc(len(a) + len(b) + 1)
	copy(buf, a)
	copy(buf[len(a):], b)
	buf[len(a)+len(b)] = 0
	return buf
}

// Printf formats a string into the heap with a trailing NUL.
func (h *Heap) Printf(format string, args ...any) []byte {
	return h.StrDup(fmt.Sprintf(format, args...))
}

// GetTop returns the latest allocation if the size matches.
func (h *Heap) GetTop(size int) []byte {
	if h == nil || size <= 0 || len(h.allocations) == 0 {
		return nil
	}
	if h.allocations[len(h.allocations)-1] != size {
		return nil
	}
	block := h.blocks[len(h.blocks)-1]
	start := block.used - size
	if start < 0 {
		return nil
	}
	return block.buf[start:block.used]
}

// FreeTop releases the latest allocation if the size matches.
func (h *Heap) FreeTop(size int) {
	if h == nil || size <= 0 || len(h.allocations) == 0 {
		return
	}
	if h.allocations[len(h.allocations)-1] != size {
		return
	}
	h.allocations = h.allocations[:len(h.allocations)-1]
	block := h.blocks[len(h.blocks)-1]
	if size > block.used {
		return
	}
	block.used -= size
	if block.used == 0 && len(h.blocks) > 1 {
		h.releaseBlock(block)
		h.blocks = h.blocks[:len(h.blocks)-1]
	}
}

// Reset clears the heap while keeping the first block.
func (h *Heap) Reset() {
	if h == nil || len(h.blocks) == 0 {
		return
	}
	for i := len(h.blocks) - 1; i > 0; i-- {
		h.releaseBlock(h.blocks[i])
	}
	h.blocks = h.blocks[:1]
	h.blocks[0].used = 0
	h.allocations = h.allocations[:0]
	h.totalSize = len(h.blocks[0].buf)
}

// Free releases all heap blocks back to the pool or GC.
func (h *Heap) Free() {
	if h == nil {
		return
	}
	for _, block := range h.blocks {
		h.releaseBlock(block)
	}
	h.blocks = nil
	h.allocations = nil
	h.totalSize = 0
}

// Size reports the total size of all heap blocks.
func (h *Heap) Size() int {
	if h == nil {
		return 0
	}
	return h.totalSize
}

func (h *Heap) nextBlockSize(minSize int) int {
	last := h.blocks[len(h.blocks)-1]
	newSize := len(last.buf) * 2
	if h.heapType&HeapBuffer != 0 {
		if newSize > MaxAllocInBuf {
			newSize = MaxAllocInBuf
		}
	} else if newSize > BlockStandardSize {
		newSize = BlockStandardSize
	}
	if newSize < minSize {
		newSize = minSize
	}
	return newSize
}

func (h *Heap) addBlock(size int) *heapBlock {
	block := &heapBlock{buf: h.allocBlock(size)}
	h.blocks = append(h.blocks, block)
	h.totalSize += len(block.buf)
	return block
}

func (h *Heap) allocBlock(size int) []byte {
	if h.pool != nil && size == h.pool.Size() {
		return h.pool.Get()
	}
	return make([]byte, size)
}

func (h *Heap) releaseBlock(block *heapBlock) {
	if block == nil {
		return
	}
	if h.pool != nil && cap(block.buf) == h.pool.Size() {
		h.pool.Put(block.buf)
	}
}

// Alloc reserves n bytes from the default allocator.
func Alloc(size int) []byte {
	return DefaultAllocator.Alloc(size)
}

// AllocZero reserves n zeroed bytes from the default allocator.
func AllocZero(size int) []byte {
	return DefaultAllocator.AllocZero(size)
}

// Free releases a buffer to the default allocator.
func Free(buf []byte) {
	DefaultAllocator.Free(buf)
}

// Dup copies data into a new allocation.
func Dup(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	buf := Alloc(len(data))
	copy(buf, data)
	return buf
}

// StrDup copies a string into a new allocation with a trailing NUL.
func StrDup(s string) []byte {
	buf := Alloc(len(s) + 1)
	copy(buf, s)
	buf[len(s)] = 0
	return buf
}

// StrDupl copies at most n bytes from s into a new allocation with a trailing NUL.
func StrDupl(s string, n int) []byte {
	if n < 0 {
		n = 0
	}
	if n > len(s) {
		n = len(s)
	}
	buf := Alloc(n + 1)
	copy(buf, s[:n])
	buf[n] = 0
	return buf
}
