package que

import "errors"

// Statement represents an executable query graph statement node.
type Statement interface {
	Node
	Execute(thr *Thr) error
}

// ErrInvalidNode reports an unexpected node type in execution.
var ErrInvalidNode = errors.New("que: invalid node")

// ThrStep executes a single node for the thread.
func ThrStep(thr *Thr) error {
	if thr == nil {
		return nil
	}
	if thr.State == ThrCompleted || thr.State == ThrError {
		return nil
	}
	if thr.RunNode == nil {
		thr.RunNode = thr.Child
	}
	if thr.RunNode == nil {
		thr.State = ThrCompleted
		thr.IsActive = false
		return nil
	}
	thr.State = ThrRunning
	thr.IsActive = true
	node := thr.RunNode
	stmt, ok := node.(Statement)
	if !ok {
		thr.State = ThrError
		thr.IsActive = false
		return ErrInvalidNode
	}
	if err := stmt.Execute(thr); err != nil {
		thr.State = ThrError
		thr.IsActive = false
		return err
	}
	thr.PrevNode = node
	thr.RunNode = node.Next()
	if thr.RunNode == nil {
		thr.State = ThrCompleted
		thr.IsActive = false
	}
	return nil
}

// ThrRun executes a thread until completion or error.
func ThrRun(thr *Thr) error {
	for thr != nil && thr.State != ThrCompleted && thr.State != ThrError {
		if err := ThrStep(thr); err != nil {
			return err
		}
	}
	return nil
}

// ForkRun executes all threads in the fork sequentially.
func ForkRun(fork *Fork) error {
	if fork == nil {
		return nil
	}
	fork.State = ForkActive
	for _, thr := range fork.Threads {
		if thr == nil {
			continue
		}
		if err := ThrRun(thr); err != nil {
			fork.State = ForkInvalid
			return err
		}
	}
	fork.State = ForkCommandWait
	return nil
}
