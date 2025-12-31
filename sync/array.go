package sync

import (
	"errors"
	stdsync "sync"
)

// ErrNoSlot reports no available slot.
var ErrNoSlot = errors.New("sync: no available slot")

// ErrInvalidSlot reports an invalid slot index.
var ErrInvalidSlot = errors.New("sync: invalid slot")

// Array is a simple wait array for thread slots.
type Array struct {
	mu    stdsync.Mutex
	slots []slot
}

type slot struct {
	inUse bool
	id    int
	ch    chan struct{}
}

// NewArray creates a new wait array.
func NewArray(size int) *Array {
	slots := make([]slot, size)
	for i := range slots {
		slots[i].ch = make(chan struct{})
	}
	return &Array{slots: slots}
}

// Reserve marks a slot as in use for a thread id.
func (a *Array) Reserve(id int) (int, error) {
	if a == nil {
		return -1, ErrNoSlot
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := range a.slots {
		if !a.slots[i].inUse {
			a.slots[i].inUse = true
			a.slots[i].id = id
			a.slots[i].ch = make(chan struct{})
			return i, nil
		}
	}
	return -1, ErrNoSlot
}

// Release frees a slot.
func (a *Array) Release(index int) error {
	if a == nil || index < 0 || index >= len(a.slots) {
		return ErrInvalidSlot
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.slots[index].inUse = false
	a.slots[index].id = 0
	return nil
}

// Wait blocks until the slot is signaled.
func (a *Array) Wait(index int) error {
	if a == nil || index < 0 || index >= len(a.slots) {
		return ErrInvalidSlot
	}
	a.mu.Lock()
	ch := a.slots[index].ch
	inUse := a.slots[index].inUse
	a.mu.Unlock()
	if !inUse {
		return ErrInvalidSlot
	}
	<-ch
	return nil
}

// Signal wakes a waiting slot.
func (a *Array) Signal(index int) error {
	if a == nil || index < 0 || index >= len(a.slots) {
		return ErrInvalidSlot
	}
	a.mu.Lock()
	ch := a.slots[index].ch
	a.mu.Unlock()
	select {
	case <-ch:
		return nil
	default:
		close(ch)
		return nil
	}
}
