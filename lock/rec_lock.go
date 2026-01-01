package lock

import (
	"time"

	"github.com/wilhasse/innodb-go/trx"
)

// LockRec acquires a record lock in the global lock system.
func LockRec(tr *trx.Trx, record RecordKey, mode Mode) (*Lock, Status) {
	sys := Sys()
	if sys == nil {
		return nil, LockGranted
	}
	return sys.LockRec(tr, record, mode)
}

// UnlockRec releases record locks in the global lock system.
func UnlockRec(tr *trx.Trx, record RecordKey) {
	sys := Sys()
	if sys == nil {
		return
	}
	sys.UnlockRec(tr, record)
}

// LockRec acquires a record lock in a lock system.
func (sys *LockSys) LockRec(tr *trx.Trx, record RecordKey, mode Mode) (*Lock, Status) {
	if sys == nil {
		return nil, LockGranted
	}
	pageKey := record.PageKey()
	heapNo := int(record.HeapNo)
	timeout := waitTimeout()
	deadline := time.Time{}
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	for {
		sys.mu.Lock()
		queue := sys.recordHash[pageKey]
		if queue == nil {
			queue = &Queue{}
			sys.recordHash[pageKey] = queue
		}
		ownGranted, ownWaiting, blockers := sys.recordBlockers(queue, tr, heapNo, mode)
		if len(blockers) == 0 {
			if ownWaiting != nil {
				ownWaiting.Flags &^= FlagWait
				ownWaiting.SetBit(heapNo)
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
				ownGranted.SetBit(heapNo)
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
			lock := &Lock{Type: LockTypeRec, Mode: mode, Trx: tr, Rec: pageKey}
			lock.SetBit(heapNo)
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
				Type:   LockTypeRec,
				Mode:   mode,
				Trx:    tr,
				Rec:    pageKey,
				Flags:  FlagWait,
				WaitCh: make(chan struct{}, 1),
			}
			waiter.SetBit(heapNo)
			queue.Append(waiter)
			sys.addLock(waiter)
		} else {
			waiter.Flags |= FlagWait
			waiter.Mode = mode
			if waiter.WaitCh == nil {
				waiter.WaitCh = make(chan struct{}, 1)
			}
			waiter.SetBit(heapNo)
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

// UnlockRec releases a record lock held by the transaction.
func (sys *LockSys) UnlockRec(tr *trx.Trx, record RecordKey) {
	if sys == nil {
		return
	}
	sys.mu.Lock()
	defer sys.mu.Unlock()

	pageKey := record.PageKey()
	queue := sys.recordHash[pageKey]
	if queue == nil {
		return
	}

	heapNo := int(record.HeapNo)
	for lock := queue.First; lock != nil; {
		next := lock.Next
		if lock.Trx == tr && lock.HasBit(heapNo) {
			lock.ClearBit(heapNo)
			if !lock.HasAnyBit() {
				queue.Remove(lock)
				sys.removeLock(lock)
			}
		}
		lock = next
	}

	sys.signalWaiters(queue)
	if queue.First == nil {
		delete(sys.recordHash, pageKey)
	}
}

func (sys *LockSys) recordBlockers(queue *Queue, tr *trx.Trx, heapNo int, mode Mode) (*Lock, *Lock, []*trx.Trx) {
	var ownGranted *Lock
	var ownWaiting *Lock
	var blockers []*trx.Trx
	for lock := queue.First; lock != nil; lock = lock.Next {
		if lock.Type != LockTypeRec {
			continue
		}
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
		if !lock.HasBit(heapNo) {
			continue
		}
		if !ModeCompatible(mode, lock.Mode) && lock.Trx != nil {
			blockers = append(blockers, lock.Trx)
		}
	}
	return ownGranted, ownWaiting, blockers
}
