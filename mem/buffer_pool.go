package mem

import "sync"

// BufferPool provides fixed-size buffers backed by sync.Pool.
type BufferPool struct {
	size int
	pool sync.Pool
}

// NewBufferPool creates a pool for buffers of a single size.
func NewBufferPool(size int) *BufferPool {
	p := &BufferPool{size: size}
	p.pool.New = func() any {
		return make([]byte, size)
	}
	return p
}

// Get returns a buffer with length equal to the pool size.
func (p *BufferPool) Get() []byte {
	buf := p.pool.Get().([]byte)
	return buf[:p.size]
}

// Put returns a buffer to the pool if it matches the pool size.
func (p *BufferPool) Put(buf []byte) {
	if cap(buf) < p.size {
		return
	}
	p.pool.Put(buf[:p.size])
}

// Size reports the pool's fixed buffer size.
func (p *BufferPool) Size() int {
	return p.size
}
