package lock

import "github.com/wilhasse/innodb-go/trx"

// LockTable acquires a table lock in the global lock system.
func LockTable(trx *trx.Trx, table string, mode Mode) (*Lock, Status) {
	sys := Sys()
	if sys == nil {
		return nil, LockGranted
	}
	return sys.LockTable(trx, table, mode)
}

// UnlockTable releases table locks in the global lock system.
func UnlockTable(trx *trx.Trx, table string) {
	sys := Sys()
	if sys == nil {
		return
	}
	sys.UnlockTable(trx, table)
}

// LockTable acquires a table lock in a lock system.
func (sys *LockSys) LockTable(trx *trx.Trx, table string, mode Mode) (*Lock, Status) {
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
	for lock := queue.First; lock != nil; lock = lock.Next {
		if lock.Trx == trx {
			own = lock
		} else if !ModeCompatible(mode, lock.Mode) {
			waiter := &Lock{Type: LockTypeTable, Mode: mode, Trx: trx, Table: table, Flags: FlagWait}
			queue.Append(waiter)
			sys.addLock(waiter)
			return waiter, LockWait
		}
	}

	if own != nil {
		if ModeStrongerOrEq(own.Mode, mode) {
			return own, LockGranted
		}
		own.Mode = mode
		return own, LockGranted
	}

	lock := &Lock{Type: LockTypeTable, Mode: mode, Trx: trx, Table: table}
	queue.Append(lock)
	sys.addLock(lock)
	return lock, LockGranted
}

// UnlockTable releases table locks held by the transaction.
func (sys *LockSys) UnlockTable(trx *trx.Trx, table string) {
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
		if lock.Trx == trx {
			queue.Remove(lock)
			sys.removeLock(lock)
		}
		lock = next
	}
	if queue.First == nil {
		delete(sys.tableHash, table)
	}
}
