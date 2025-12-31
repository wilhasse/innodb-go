package btr

import "sync"

var (
	hashBuiltMu sync.Mutex
	hashBuilt   = map[*Tree]struct{}{}
)

func markHashBuilt(tree *Tree) {
	if tree == nil {
		return
	}
	hashBuiltMu.Lock()
	hashBuilt[tree] = struct{}{}
	hashBuiltMu.Unlock()
}

func hashBuiltEnabled(tree *Tree) bool {
	if tree == nil {
		return false
	}
	hashBuiltMu.Lock()
	_, ok := hashBuilt[tree]
	hashBuiltMu.Unlock()
	return ok
}

func clearHashBuilt() {
	hashBuiltMu.Lock()
	hashBuilt = map[*Tree]struct{}{}
	hashBuiltMu.Unlock()
}

func updateSearchEntry(tree *Tree, key []byte, node *node, index int) {
	if tree == nil || node == nil || !searchEnabled() || searchSys == nil || !hashBuiltEnabled(tree) {
		return
	}
	searchSys.mu.Lock()
	searchSys.entries[searchKey{tree: tree, key: string(key)}] = searchEntry{
		node:     node,
		index:    index,
		modCount: tree.modCount,
	}
	searchSys.mu.Unlock()
}

func removeSearchEntry(tree *Tree, key []byte) {
	if tree == nil || !searchEnabled() || searchSys == nil || !hashBuiltEnabled(tree) {
		return
	}
	searchSys.mu.Lock()
	delete(searchSys.entries, searchKey{tree: tree, key: string(key)})
	searchSys.mu.Unlock()
}
