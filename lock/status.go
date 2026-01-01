package lock

// Status reports the outcome of a lock request.
type Status int

const (
	LockGranted Status = iota
	LockWait
	LockDeadlock
)
