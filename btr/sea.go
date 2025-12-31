package btr

import (
	"sync"
)

// Adaptive search constants from btr0sea.c.
const (
	BtrSearchPageBuildLimit = 16
	BtrSearchBuildLimit     = 100
)

// SearchEnabled mirrors btr_search_enabled.
var SearchEnabled = true

// SearchThisIsZero mirrors btr_search_this_is_zero.
var SearchThisIsZero uint64

// SearchNSucc counts successful adaptive hash lookups.
var SearchNSucc uint64

// SearchNHashFail counts failed adaptive hash lookups.
var SearchNHashFail uint64

type searchKey struct {
	tree *Tree
	key  string
}

type searchEntry struct {
	node     *node
	index    int
	modCount uint64
}

// SearchSys is a simplified adaptive search system.
type SearchSys struct {
	mu         sync.RWMutex
	entries    map[searchKey]searchEntry
	maxEntries int
}

var searchSys *SearchSys
var searchEnabledMu sync.RWMutex

// SearchVarInit resets global adaptive search variables.
func SearchVarInit() {
	SearchThisIsZero = 0
	SearchNSucc = 0
	SearchNHashFail = 0
	searchSys = nil
}

// SearchSysCreate initializes the adaptive search system.
func SearchSysCreate(maxEntries int) {
	if maxEntries <= 0 {
		maxEntries = 1024
	}
	searchSys = &SearchSys{
		entries:    make(map[searchKey]searchEntry, maxEntries),
		maxEntries: maxEntries,
	}
}

// SearchSysClose tears down the adaptive search system.
func SearchSysClose() {
	searchSys = nil
}

// SearchEnable toggles adaptive search on.
func SearchEnable() {
	searchEnabledMu.Lock()
	SearchEnabled = true
	searchEnabledMu.Unlock()
}

// SearchDisable toggles adaptive search off.
func SearchDisable() {
	searchEnabledMu.Lock()
	SearchEnabled = false
	searchEnabledMu.Unlock()
}

func searchEnabled() bool {
	searchEnabledMu.RLock()
	enabled := SearchEnabled
	searchEnabledMu.RUnlock()
	return enabled
}

// AdaptiveSearchCursor returns a cursor if the key is found.
func AdaptiveSearchCursor(tree *Tree, key []byte) (*Cursor, bool) {
	if tree == nil {
		return nil, false
	}
	if !searchEnabled() || searchSys == nil {
		return exactCursor(tree, key)
	}

	k := searchKey{tree: tree, key: string(key)}

	searchSys.mu.RLock()
	entry, ok := searchSys.entries[k]
	searchSys.mu.RUnlock()

	if ok && entry.modCount == tree.modCount {
		SearchNSucc++
		return &Cursor{node: entry.node, index: entry.index}, true
	}

	SearchNHashFail++
	cur, found := exactCursor(tree, key)
	if found {
		searchSys.mu.Lock()
		searchSys.entries[k] = searchEntry{
			node:     cur.node,
			index:    cur.index,
			modCount: tree.modCount,
		}
		if len(searchSys.entries) > searchSys.maxEntries {
			searchSys.entries = make(map[searchKey]searchEntry, searchSys.maxEntries)
		}
		searchSys.mu.Unlock()
	}
	return cur, found
}

// AdaptiveSearch returns the value for the key if found.
func AdaptiveSearch(tree *Tree, key []byte) ([]byte, bool) {
	cur, ok := AdaptiveSearchCursor(tree, key)
	if !ok || cur == nil || !cur.Valid() {
		return nil, false
	}
	return cloneBytes(cur.node.values[cur.index]), true
}

func exactCursor(tree *Tree, key []byte) (*Cursor, bool) {
	leaf := tree.findLeaf(key)
	if leaf == nil {
		return nil, false
	}
	idx := tree.keyIndex(leaf.keys, key)
	if idx < len(leaf.keys) && tree.compare(leaf.keys[idx], key) == 0 {
		return &Cursor{node: leaf, index: idx}, true
	}
	return nil, false
}
