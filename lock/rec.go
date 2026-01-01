package lock

// SetBit sets the record bit for a heap number.
func (lock *Lock) SetBit(heapNo int) {
	if lock == nil || heapNo < 0 {
		return
	}
	if heapNo >= len(lock.Bits) {
		newBits := make([]bool, heapNo+1)
		copy(newBits, lock.Bits)
		lock.Bits = newBits
	}
	lock.Bits[heapNo] = true
}

// ClearBit clears the record bit for a heap number.
func (lock *Lock) ClearBit(heapNo int) {
	if lock == nil || heapNo < 0 || heapNo >= len(lock.Bits) {
		return
	}
	lock.Bits[heapNo] = false
}

// HasBit reports whether the heap bit is set.
func (lock *Lock) HasBit(heapNo int) bool {
	if lock == nil || heapNo < 0 || heapNo >= len(lock.Bits) {
		return false
	}
	return lock.Bits[heapNo]
}

// RecLockFirst returns the first lock in the queue that has the heap bit set.
func RecLockFirst(queue *Queue, heapNo int) *Lock {
	if queue == nil || heapNo < 0 {
		return nil
	}
	for lock := queue.First; lock != nil; lock = lock.Next {
		if lock.Type == LockRec && lock.HasBit(heapNo) {
			return lock
		}
	}
	return nil
}

// RecLockNext returns the next lock after the given one with the heap bit set.
func RecLockNext(lock *Lock, heapNo int) *Lock {
	if lock == nil || heapNo < 0 {
		return nil
	}
	for next := lock.Next; next != nil; next = next.Next {
		if next.Type == LockRec && next.HasBit(heapNo) {
			return next
		}
	}
	return nil
}
