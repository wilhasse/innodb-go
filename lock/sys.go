package lock

import (
	"sync"

	"github.com/wilhasse/innodb-go/trx"
)

// LockSys stores the global lock hash tables.
type LockSys struct {
	mu         sync.Mutex
	tableHash  map[string]*Queue
	recordHash map[RecordPageKey]*Queue
	trxLocks   map[*trx.Trx]map[*Lock]struct{}
}

var system *LockSys

// SysCreate initializes the global lock system.
func SysCreate(_ int) {
	system = NewLockSys()
}

// SysClose shuts down the global lock system.
func SysClose() {
	system = nil
}

// Sys returns the global lock system.
func Sys() *LockSys {
	return system
}

// NewLockSys creates a standalone lock system.
func NewLockSys() *LockSys {
	return &LockSys{
		tableHash:  make(map[string]*Queue),
		recordHash: make(map[RecordPageKey]*Queue),
		trxLocks:   make(map[*trx.Trx]map[*Lock]struct{}),
	}
}

// NewManager returns a lock system (compatibility alias).
func NewManager() *LockSys {
	return NewLockSys()
}

func (sys *LockSys) addLock(lock *Lock) {
	if sys == nil || lock == nil || lock.Trx == nil {
		return
	}
	locks := sys.trxLocks[lock.Trx]
	if locks == nil {
		locks = make(map[*Lock]struct{})
		sys.trxLocks[lock.Trx] = locks
	}
	locks[lock] = struct{}{}
}

func (sys *LockSys) removeLock(lock *Lock) {
	if sys == nil || lock == nil || lock.Trx == nil {
		return
	}
	if locks := sys.trxLocks[lock.Trx]; locks != nil {
		delete(locks, lock)
		if len(locks) == 0 {
			delete(sys.trxLocks, lock.Trx)
		}
	}
}
