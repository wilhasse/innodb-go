package sync

import stdsync "sync"

// Locker mirrors sync.Locker.
type Locker = stdsync.Locker

// Mutex mirrors sync.Mutex.
type Mutex = stdsync.Mutex

// RWMutex mirrors sync.RWMutex.
type RWMutex = stdsync.RWMutex

// Cond mirrors sync.Cond.
type Cond = stdsync.Cond

// WaitGroup mirrors sync.WaitGroup.
type WaitGroup = stdsync.WaitGroup

// Once mirrors sync.Once.
type Once = stdsync.Once

// NewCond mirrors sync.NewCond.
func NewCond(l Locker) *Cond {
	return stdsync.NewCond(l)
}
