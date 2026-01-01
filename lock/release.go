package lock

import "github.com/wilhasse/innodb-go/trx"

// ReleaseAll releases all locks for the transaction in the global system.
func ReleaseAll(tr *trx.Trx) {
	sys := Sys()
	if sys == nil {
		return
	}
	sys.ReleaseAll(tr)
}

// ReleaseAll releases all locks for the transaction in a lock system.
func (sys *LockSys) ReleaseAll(tr *trx.Trx) {
	if sys == nil || tr == nil {
		return
	}
	sys.mu.Lock()
	defer sys.mu.Unlock()

	locks := sys.trxLocks[tr]
	if len(locks) == 0 {
		sys.clearWaitEdges(tr)
		return
	}
	list := make([]*Lock, 0, len(locks))
	for lock := range locks {
		list = append(list, lock)
	}
	for _, lock := range list {
		switch lock.Type {
		case LockTypeTable:
			if queue := sys.tableHash[lock.Table]; queue != nil {
				queue.Remove(lock)
				if queue.First == nil {
					delete(sys.tableHash, lock.Table)
				}
			}
		case LockTypeRec:
			if queue := sys.recordHash[lock.Rec]; queue != nil {
				queue.Remove(lock)
				if queue.First == nil {
					delete(sys.recordHash, lock.Rec)
				}
			}
		}
	}
	delete(sys.trxLocks, tr)
	sys.clearWaitEdges(tr)
}
