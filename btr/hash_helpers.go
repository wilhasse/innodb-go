package btr

// SearchBuildPageHashIndex prebuilds the adaptive hash entries for a tree.
func SearchBuildPageHashIndex(tree *Tree) int {
	if tree == nil || !searchEnabled() {
		return 0
	}
	if searchSys == nil {
		SearchSysCreate(1024)
	}

	searchSys.mu.Lock()
	defer searchSys.mu.Unlock()
	for k := range searchSys.entries {
		if k.tree == tree {
			delete(searchSys.entries, k)
		}
	}

	count := 0
	cur := tree.First()
	for cur != nil && cur.Valid() {
		key := cur.Key()
		if !tree.isDeleted(key) {
			searchSys.entries[searchKey{tree: tree, key: string(key)}] = searchEntry{
				node:     cur.node,
				index:    cur.index,
				modCount: tree.modCount,
			}
			count++
		}
		if !cur.Next() {
			break
		}
	}
	markHashBuilt(tree)
	return count
}

// SearchGuessOnHash attempts a lookup via the adaptive hash entries only.
func SearchGuessOnHash(tree *Tree, key []byte) ([]byte, bool) {
	if tree == nil || !searchEnabled() || searchSys == nil {
		return nil, false
	}
	k := searchKey{tree: tree, key: string(key)}

	searchSys.mu.RLock()
	entry, ok := searchSys.entries[k]
	searchSys.mu.RUnlock()

	if ok && entry.modCount == tree.modCount && entry.node != nil &&
		entry.index >= 0 && entry.index < len(entry.node.keys) &&
		tree.compare(entry.node.keys[entry.index], key) == 0 &&
		!tree.isDeleted(key) {
		SearchNSucc++
		return cloneBytes(entry.node.values[entry.index]), true
	}

	SearchNHashFail++
	return nil, false
}
