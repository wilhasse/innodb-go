package mem

// Allocator defines the allocation contract used by the Go port.
type Allocator interface {
	Alloc(size int) []byte
	AllocZero(size int) []byte
	Free(buf []byte)
}

// GoAllocator delegates to the Go runtime and keeps Free as a no-op.
type GoAllocator struct{}

func (GoAllocator) Alloc(size int) []byte {
	return make([]byte, size)
}

func (GoAllocator) AllocZero(size int) []byte {
	return make([]byte, size)
}

func (GoAllocator) Free([]byte) {}

// DefaultAllocator is the global allocator used by the port unless overridden.
var DefaultAllocator Allocator = GoAllocator{}
