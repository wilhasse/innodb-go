package lock

import "github.com/wilhasse/innodb-go/trx"

// LockRec acquires a record lock in the global lock system.
func LockRec(trx *trx.Trx, record RecordKey, mode Mode) (*Lock, Status) {
	sys := Sys()
	if sys == nil {
		return nil, LockGranted
	}
	return sys.LockRec(trx, record, mode)
}

// UnlockRec releases record locks in the global lock system.
func UnlockRec(trx *trx.Trx, record RecordKey) {
	sys := Sys()
	if sys == nil {
		return
	}
	sys.UnlockRec(trx, record)
}

// LockRec acquires a record lock in a lock system.
func (sys *LockSys) LockRec(trx *trx.Trx, record RecordKey, mode Mode) (*Lock, Status) {
	if sys == nil {
		return nil, LockGranted
	}
	sys.mu.Lock()
	defer sys.mu.Unlock()

	pageKey := record.PageKey()
	queue := sys.recordHash[pageKey]
	if queue == nil {
		queue = &Queue{}
		sys.recordHash[pageKey] = queue
	}

	heapNo := int(record.HeapNo)
	var own *Lock
	for lock := queue.First; lock != nil; lock = lock.Next {
		if lock.Type != LockTypeRec {
			continue
		}
		if lock.Trx == trx && own == nil {
			own = lock
		}
		if !lock.HasBit(heapNo) {
			continue
		}
		if lock.Trx == trx {
			continue
		}
		if !ModeCompatible(mode, lock.Mode) {
			waiter := &Lock{Type: LockTypeRec, Mode: mode, Trx: trx, Rec: pageKey, Flags: FlagWait}
			waiter.SetBit(heapNo)
			queue.Append(waiter)
			sys.addLock(waiter)
			return waiter, LockWait
		}
	}

	if own != nil {
		own.SetBit(heapNo)
		if ModeStrongerOrEq(own.Mode, mode) {
			return own, LockGranted
		}
		own.Mode = mode
		return own, LockGranted
	}

	lock := &Lock{Type: LockTypeRec, Mode: mode, Trx: trx, Rec: pageKey}
	lock.SetBit(heapNo)
	queue.Append(lock)
	sys.addLock(lock)
	return lock, LockGranted
}

// UnlockRec releases a record lock held by the transaction.
func (sys *LockSys) UnlockRec(trx *trx.Trx, record RecordKey) {
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
		if lock.Trx == trx && lock.HasBit(heapNo) {
			lock.ClearBit(heapNo)
			if !lock.HasAnyBit() {
				queue.Remove(lock)
				sys.removeLock(lock)
			}
		}
		lock = next
	}

	if queue.First == nil {
		delete(sys.recordHash, pageKey)
	}
}
