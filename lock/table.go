package lock

import (
	"time"

	"github.com/wilhasse/innodb-go/trx"
)

// LockTable acquires a table lock in the global lock system.
func LockTable(tr *trx.Trx, table string, mode Mode) (*Lock, Status) {
	sys := Sys()
	if sys == nil {
		return nil, LockGranted
	}
	return sys.LockTable(tr, table, mode)
}

// UnlockTable releases table locks in the global lock system.
func UnlockTable(tr *trx.Trx, table string) {
	sys := Sys()
	if sys == nil {
		return
	}
	sys.UnlockTable(tr, table)
}

// LockTable acquires a table lock in a lock system.
func (sys *LockSys) LockTable(tr *trx.Trx, table string, mode Mode) (*Lock, Status) {
	if sys == nil {
		return nil, LockGranted
	}
	timeout := waitTimeout()
	deadline := time.Time{}
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	for {
		sys.mu.Lock()
		queue := sys.tableHash[table]
		if queue == nil {
			queue = &Queue{}
			sys.tableHash[table] = queue
		}
		ownGranted, ownWaiting, blockers := sys.tableBlockers(queue, tr, mode)
		if len(blockers) == 0 {
			if ownWaiting != nil {
				ownWaiting.Flags &^= FlagWait
				if ModeStrongerOrEq(ownWaiting.Mode, mode) {
					sys.clearWaitEdges(tr)
					sys.mu.Unlock()
					return ownWaiting, LockGranted
				}
				ownWaiting.Mode = mode
				sys.clearWaitEdges(tr)
				sys.mu.Unlock()
				return ownWaiting, LockGranted
			}
			if ownGranted != nil {
				ownGranted.Flags &^= FlagWait
				if ModeStrongerOrEq(ownGranted.Mode, mode) {
					sys.clearWaitEdges(tr)
					sys.mu.Unlock()
					return ownGranted, LockGranted
				}
				ownGranted.Mode = mode
				sys.clearWaitEdges(tr)
				sys.mu.Unlock()
				return ownGranted, LockGranted
			}
			lock := &Lock{Type: LockTypeTable, Mode: mode, Trx: tr, Table: table}
			queue.Append(lock)
			sys.addLock(lock)
			sys.clearWaitEdges(tr)
			sys.mu.Unlock()
			return lock, LockGranted
		}

		sys.clearWaitEdges(tr)
		for _, blocker := range blockers {
			sys.addWaitEdge(tr, blocker)
		}
		if sys.deadlock(tr) {
			sys.clearWaitEdges(tr)
			sys.mu.Unlock()
			return nil, LockDeadlock
		}
		if timeout <= 0 {
			sys.clearWaitEdges(tr)
			sys.mu.Unlock()
			return nil, LockWaitTimeout
		}
		waiter := ownWaiting
		if waiter == nil {
			waiter = &Lock{
				Type:   LockTypeTable,
				Mode:   mode,
				Trx:    tr,
				Table:  table,
				Flags:  FlagWait,
				WaitCh: make(chan struct{}, 1),
			}
			queue.Append(waiter)
			sys.addLock(waiter)
		} else {
			waiter.Flags |= FlagWait
			waiter.Mode = mode
			if waiter.WaitCh == nil {
				waiter.WaitCh = make(chan struct{}, 1)
			}
		}
		waitCh := waiter.WaitCh
		sys.mu.Unlock()

		if !waitForSignal(waitCh, deadline) {
			sys.mu.Lock()
			sys.removeLockFromQueue(waiter)
			sys.clearWaitEdges(tr)
			sys.mu.Unlock()
			return nil, LockWaitTimeout
		}
	}
}

// UnlockTable releases table locks held by the transaction.
func (sys *LockSys) UnlockTable(tr *trx.Trx, table string) {
	if sys == nil {
		return
	}
	sys.mu.Lock()
	defer sys.mu.Unlock()

	queue := sys.tableHash[table]
	if queue == nil {
		return
	}
	for lock := queue.First; lock != nil; {
		next := lock.Next
		if lock.Trx == tr {
			queue.Remove(lock)
			sys.removeLock(lock)
		}
		lock = next
	}
	sys.signalWaiters(queue)
	if queue.First == nil {
		delete(sys.tableHash, table)
	}
}

func (sys *LockSys) tableBlockers(queue *Queue, tr *trx.Trx, mode Mode) (*Lock, *Lock, []*trx.Trx) {
	var ownGranted *Lock
	var ownWaiting *Lock
	var blockers []*trx.Trx
	for lock := queue.First; lock != nil; lock = lock.Next {
		if lock.Trx == tr {
			if lock.Flags&FlagWait != 0 {
				if ownWaiting == nil {
					ownWaiting = lock
				}
			} else if ownGranted == nil {
				ownGranted = lock
			}
			continue
		}
		if lock.Flags&FlagWait != 0 {
			continue
		}
		if !ModeCompatible(mode, lock.Mode) && lock.Trx != nil {
			blockers = append(blockers, lock.Trx)
		}
	}
	return ownGranted, ownWaiting, blockers
}
