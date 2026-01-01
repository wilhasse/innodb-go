package lock

import "github.com/wilhasse/innodb-go/trx"

func (sys *LockSys) addWaitEdge(waiter, blocker *trx.Trx) {
	if sys == nil || waiter == nil || blocker == nil || waiter == blocker {
		return
	}
	edges := sys.waitFor[waiter]
	if edges == nil {
		edges = make(map[*trx.Trx]struct{})
		sys.waitFor[waiter] = edges
	}
	edges[blocker] = struct{}{}
}

func (sys *LockSys) clearWaitEdges(waiter *trx.Trx) {
	if sys == nil || waiter == nil {
		return
	}
	delete(sys.waitFor, waiter)
}

func (sys *LockSys) deadlock(waiter *trx.Trx) bool {
	if sys == nil || waiter == nil {
		return false
	}
	blockers := sys.waitFor[waiter]
	if len(blockers) == 0 {
		return false
	}
	seen := make(map[*trx.Trx]struct{})
	for blocker := range blockers {
		if sys.hasPath(blocker, waiter, seen) {
			return true
		}
	}
	return false
}

func (sys *LockSys) hasPath(from, target *trx.Trx, seen map[*trx.Trx]struct{}) bool {
	if from == nil || target == nil {
		return false
	}
	if from == target {
		return true
	}
	if _, ok := seen[from]; ok {
		return false
	}
	seen[from] = struct{}{}
	for next := range sys.waitFor[from] {
		if sys.hasPath(next, target, seen) {
			return true
		}
	}
	return false
}
