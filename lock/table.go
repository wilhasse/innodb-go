package lock

import "github.com/wilhasse/innodb-go/trx"

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
	sys.mu.Lock()
	defer sys.mu.Unlock()

	queue := sys.tableHash[table]
	if queue == nil {
		queue = &Queue{}
		sys.tableHash[table] = queue
	}

	var own *Lock
	var blockers []*trx.Trx
	for lock := queue.First; lock != nil; lock = lock.Next {
		if lock.Trx == tr {
			own = lock
			continue
		}
		if !ModeCompatible(mode, lock.Mode) && lock.Trx != nil {
			blockers = append(blockers, lock.Trx)
		}
	}

	if len(blockers) > 0 {
		for _, blocker := range blockers {
			sys.addWaitEdge(tr, blocker)
		}
		if sys.deadlock(tr) {
			sys.clearWaitEdges(tr)
			return nil, LockDeadlock
		}
		waiter := &Lock{Type: LockTypeTable, Mode: mode, Trx: tr, Table: table, Flags: FlagWait}
		queue.Append(waiter)
		sys.addLock(waiter)
		return waiter, LockWait
	}

	if own != nil {
		if ModeStrongerOrEq(own.Mode, mode) {
			sys.clearWaitEdges(tr)
			return own, LockGranted
		}
		own.Mode = mode
		sys.clearWaitEdges(tr)
		return own, LockGranted
	}

	lock := &Lock{Type: LockTypeTable, Mode: mode, Trx: tr, Table: table}
	queue.Append(lock)
	sys.addLock(lock)
	sys.clearWaitEdges(tr)
	return lock, LockGranted
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
	if queue.First == nil {
		delete(sys.tableHash, table)
	}
}
