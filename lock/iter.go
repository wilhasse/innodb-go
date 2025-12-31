package lock

// UndefinedBitNo mirrors ULINT_UNDEFINED for record bits.
const UndefinedBitNo = -1

// LockType distinguishes table and record locks.
type LockType int

const (
	LockTable LockType = iota
	LockRec
)

// Lock represents a simplified lock node.
type Lock struct {
	Type LockType
	Prev *Lock
	Next *Lock
	Bits []bool
}

// QueueIterator iterates over a lock queue.
type QueueIterator struct {
	Current *Lock
	BitNo   int
}

// Reset initializes the iterator from a lock.
func (it *QueueIterator) Reset(lock *Lock, bitNo int) {
	if it == nil {
		return
	}
	it.Current = lock
	if lock == nil {
		it.BitNo = UndefinedBitNo
		return
	}
	if bitNo != UndefinedBitNo {
		it.BitNo = bitNo
		return
	}
	switch lock.Type {
	case LockTable:
		it.BitNo = UndefinedBitNo
	case LockRec:
		it.BitNo = findSetBit(lock)
	default:
		it.BitNo = UndefinedBitNo
	}
}

// GetPrev moves to the previous lock in the queue.
func (it *QueueIterator) GetPrev() *Lock {
	if it == nil || it.Current == nil {
		return nil
	}
	var prev *Lock
	switch it.Current.Type {
	case LockRec, LockTable:
		prev = it.Current.Prev
	}
	if prev != nil {
		it.Current = prev
	}
	return prev
}

func findSetBit(lock *Lock) int {
	if lock == nil {
		return UndefinedBitNo
	}
	for i, set := range lock.Bits {
		if set {
			return i
		}
	}
	return UndefinedBitNo
}
