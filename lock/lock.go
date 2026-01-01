package lock

import (
	"unsafe"

	"github.com/wilhasse/innodb-go/trx"
)

// RecordKey identifies a record within a table.
type RecordKey struct {
	Table  string
	PageNo uint32
	RecID  uint32
}

// Queue holds a lock queue.
type Queue struct {
	First *Lock
	Last  *Lock
}

// GetSize returns the size of a lock struct in bytes.
func GetSize() int {
	return int(unsafe.Sizeof(Lock{}))
}

// AcquireTableLock appends a table lock to the queue.
func (sys *LockSys) AcquireTableLock(trx *trx.Trx, table string, mode Mode) *Lock {
	if sys == nil {
		return nil
	}
	sys.mu.Lock()
	defer sys.mu.Unlock()
	lock := &Lock{Type: LockTable, Mode: mode, Trx: trx, Table: table}
	queue := sys.tableHash[table]
	if queue == nil {
		queue = &Queue{}
		sys.tableHash[table] = queue
	}
	queue.Append(lock)
	sys.addLock(lock)
	return lock
}

// AcquireRecordLock appends a record lock to the queue.
func (sys *LockSys) AcquireRecordLock(trx *trx.Trx, record RecordKey, mode Mode) *Lock {
	if sys == nil {
		return nil
	}
	sys.mu.Lock()
	defer sys.mu.Unlock()
	lock := &Lock{Type: LockRec, Mode: mode, Trx: trx, Record: record}
	queue := sys.recordHash[record]
	if queue == nil {
		queue = &Queue{}
		sys.recordHash[record] = queue
	}
	queue.Append(lock)
	sys.addLock(lock)
	return lock
}

// Release removes a lock from its queue.
func (sys *LockSys) Release(lock *Lock) {
	if sys == nil || lock == nil {
		return
	}
	sys.mu.Lock()
	defer sys.mu.Unlock()
	var queue *Queue
	switch lock.Type {
	case LockTable:
		queue = sys.tableHash[lock.Table]
	case LockRec:
		queue = sys.recordHash[lock.Record]
	}
	if queue == nil {
		return
	}
	queue.Remove(lock)
	sys.removeLock(lock)
}

// TableQueue returns the queue for a table.
func (sys *LockSys) TableQueue(table string) *Queue {
	if sys == nil {
		return nil
	}
	sys.mu.Lock()
	defer sys.mu.Unlock()
	return sys.tableHash[table]
}

// RecordQueue returns the queue for a record.
func (sys *LockSys) RecordQueue(record RecordKey) *Queue {
	if sys == nil {
		return nil
	}
	sys.mu.Lock()
	defer sys.mu.Unlock()
	return sys.recordHash[record]
}

// Append adds a lock to the queue tail.
func (q *Queue) Append(lock *Lock) {
	if q == nil || lock == nil {
		return
	}
	if q.Last == nil {
		q.First = lock
		q.Last = lock
		lock.Prev = nil
		lock.Next = nil
		return
	}
	lock.Prev = q.Last
	lock.Next = nil
	q.Last.Next = lock
	q.Last = lock
}

// Remove removes a lock from the queue.
func (q *Queue) Remove(lock *Lock) {
	if q == nil || lock == nil {
		return
	}
	if lock.Prev != nil {
		lock.Prev.Next = lock.Next
	} else {
		q.First = lock.Next
	}
	if lock.Next != nil {
		lock.Next.Prev = lock.Prev
	} else {
		q.Last = lock.Prev
	}
	lock.Prev = nil
	lock.Next = nil
}
