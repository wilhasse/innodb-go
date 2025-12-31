package lock

import (
	"sync"
	"unsafe"
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

// Manager stores lock queues for tables and records.
type Manager struct {
	mu           sync.Mutex
	tableQueues  map[string]*Queue
	recordQueues map[RecordKey]*Queue
}

var system *Manager

// GetSize returns the size of a lock struct in bytes.
func GetSize() int {
	return int(unsafe.Sizeof(Lock{}))
}

// SysCreate initializes the global lock system.
func SysCreate(_ int) {
	system = &Manager{
		tableQueues:  make(map[string]*Queue),
		recordQueues: make(map[RecordKey]*Queue),
	}
}

// SysClose shuts down the global lock system.
func SysClose() {
	system = nil
}

// Sys returns the global lock manager.
func Sys() *Manager {
	return system
}

// NewManager creates a standalone lock manager.
func NewManager() *Manager {
	return &Manager{
		tableQueues:  make(map[string]*Queue),
		recordQueues: make(map[RecordKey]*Queue),
	}
}

// AcquireTableLock appends a table lock to the queue.
func (m *Manager) AcquireTableLock(trxID, table string, mode Mode) *Lock {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	lock := &Lock{Type: LockTable, Mode: mode, TrxID: trxID, Table: table}
	queue := m.tableQueues[table]
	if queue == nil {
		queue = &Queue{}
		m.tableQueues[table] = queue
	}
	queue.Append(lock)
	return lock
}

// AcquireRecordLock appends a record lock to the queue.
func (m *Manager) AcquireRecordLock(trxID string, record RecordKey, mode Mode) *Lock {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	lock := &Lock{Type: LockRec, Mode: mode, TrxID: trxID, Record: record}
	queue := m.recordQueues[record]
	if queue == nil {
		queue = &Queue{}
		m.recordQueues[record] = queue
	}
	queue.Append(lock)
	return lock
}

// Release removes a lock from its queue.
func (m *Manager) Release(lock *Lock) {
	if m == nil || lock == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var queue *Queue
	switch lock.Type {
	case LockTable:
		queue = m.tableQueues[lock.Table]
	case LockRec:
		queue = m.recordQueues[lock.Record]
	}
	if queue == nil {
		return
	}
	queue.Remove(lock)
}

// TableQueue returns the queue for a table.
func (m *Manager) TableQueue(table string) *Queue {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tableQueues[table]
}

// RecordQueue returns the queue for a record.
func (m *Manager) RecordQueue(record RecordKey) *Queue {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.recordQueues[record]
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
