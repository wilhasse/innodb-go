package lock

func (sys *LockSys) signalWaiters(queue *Queue) {
	if sys == nil || queue == nil {
		return
	}
	for lock := queue.First; lock != nil; lock = lock.Next {
		if lock.Flags&FlagWait == 0 || lock.WaitCh == nil {
			continue
		}
		select {
		case lock.WaitCh <- struct{}{}:
		default:
		}
	}
}

func (sys *LockSys) removeLockFromQueue(lock *Lock) {
	if sys == nil || lock == nil {
		return
	}
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
	sys.removeLock(lock)
}
