package dyn

// DynArrayDataSize mirrors DYN_ARRAY_DATA_SIZE.
const DynArrayDataSize = 512

// DynBlock represents a block in a dynamic array.
type DynBlock struct {
	data []byte
	used int
}

// Used returns the number of bytes used in the block.
func (b *DynBlock) Used() int {
	if b == nil {
		return 0
	}
	return b.used
}

// Data returns the underlying block data.
func (b *DynBlock) Data() []byte {
	if b == nil {
		return nil
	}
	return b.data
}

type openState struct {
	block *DynBlock
	start int
	size  int
}

// Array is a dynamically allocated byte array.
type Array struct {
	blocks []*DynBlock
	open   *openState
}

// New creates a dynamic array with an initial block.
func New() *Array {
	arr := &Array{}
	arr.AddBlock()
	return arr
}

// Free releases the array contents.
func (a *Array) Free() {
	if a == nil {
		return
	}
	a.blocks = nil
	a.open = nil
}

// AddBlock appends a new block and returns it.
func (a *Array) AddBlock() *DynBlock {
	if a == nil {
		return nil
	}
	block := &DynBlock{data: make([]byte, DynArrayDataSize)}
	a.blocks = append(a.blocks, block)
	return block
}

// FirstBlock returns the first block in the array.
func (a *Array) FirstBlock() *DynBlock {
	if a == nil || len(a.blocks) == 0 {
		return nil
	}
	return a.blocks[0]
}

// LastBlock returns the last block in the array.
func (a *Array) LastBlock() *DynBlock {
	if a == nil || len(a.blocks) == 0 {
		return nil
	}
	return a.blocks[len(a.blocks)-1]
}

// NextBlock returns the next block after the provided block.
func (a *Array) NextBlock(block *DynBlock) *DynBlock {
	if a == nil || block == nil {
		return nil
	}
	for i, b := range a.blocks {
		if b == block && i+1 < len(a.blocks) {
			return a.blocks[i+1]
		}
	}
	return nil
}

// Push reserves size bytes and returns a slice for writing.
func (a *Array) Push(size int) []byte {
	if a == nil || size <= 0 || size > DynArrayDataSize {
		return nil
	}
	block := a.LastBlock()
	if block == nil {
		block = a.AddBlock()
	}
	if block.used+size > DynArrayDataSize {
		block = a.AddBlock()
	}
	start := block.used
	block.used += size
	return block.data[start:block.used]
}

// Open reserves size bytes and returns a slice for writing.
func (a *Array) Open(size int) []byte {
	buf := a.Push(size)
	if a == nil || buf == nil {
		return nil
	}
	block := a.LastBlock()
	a.open = &openState{
		block: block,
		start: block.used - size,
		size:  size,
	}
	return buf
}

// Close adjusts the last open reservation to the actual used length.
func (a *Array) Close(used int) {
	if a == nil || a.open == nil {
		return
	}
	if used < 0 {
		used = 0
	}
	if used > a.open.size {
		used = a.open.size
	}
	a.open.block.used = a.open.start + used
	a.open = nil
}

// PushBytes appends a byte slice to the array.
func (a *Array) PushBytes(data []byte) {
	if a == nil || len(data) == 0 {
		return
	}
	offset := 0
	for offset < len(data) {
		remaining := len(data) - offset
		chunk := remaining
		if chunk > DynArrayDataSize {
			chunk = DynArrayDataSize
		}
		buf := a.Push(chunk)
		if buf == nil {
			return
		}
		copy(buf, data[offset:offset+chunk])
		offset += chunk
	}
}

// GetElement returns a slice starting at the given byte offset.
func (a *Array) GetElement(pos int) []byte {
	if a == nil || pos < 0 {
		return nil
	}
	offset := pos
	for _, block := range a.blocks {
		if offset < block.used {
			return block.data[offset:block.used]
		}
		offset -= block.used
	}
	return nil
}

// DataSize returns the total used data size.
func (a *Array) DataSize() int {
	if a == nil {
		return 0
	}
	total := 0
	for _, block := range a.blocks {
		total += block.used
	}
	return total
}
