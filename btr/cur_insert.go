package btr

// InsertIfPossible tries to insert without splits; returns false on duplicate or no space.
func (c *Cur) InsertIfPossible(key, value []byte) bool {
	if c == nil || c.Tree == nil {
		return false
	}
	if c.Tree.root == nil {
		c.Tree.root = &node{
			leaf:   true,
			keys:   [][]byte{cloneBytes(key)},
			values: [][]byte{cloneBytes(value)},
		}
		c.Tree.size = 1
		c.Tree.modCount++
		c.Cursor = &Cursor{node: c.Tree.root, index: 0}
		return true
	}

	leaf := c.Tree.findLeaf(key)
	if leaf == nil {
		return false
	}
	idx := c.Tree.keyIndex(leaf.keys, key)
	if idx < len(leaf.keys) && c.Tree.compare(leaf.keys[idx], key) == 0 {
		c.Cursor = &Cursor{node: leaf, index: idx}
		return false
	}
	if len(leaf.keys) >= c.Tree.maxKeys() {
		return false
	}

	leaf.keys = insertBytes(leaf.keys, idx, cloneBytes(key))
	leaf.values = insertBytes(leaf.values, idx, cloneBytes(value))
	c.Tree.size++
	c.Tree.modCount++
	if idx == 0 {
		c.Tree.updateParentKey(leaf)
	}
	c.Cursor = &Cursor{node: leaf, index: idx}
	return true
}

// OptimisticInsert inserts into a leaf without split paths.
func (c *Cur) OptimisticInsert(key, value []byte) bool {
	if c == nil {
		return false
	}
	c.InsLockAndUndo()
	ok := c.InsertIfPossible(key, value)
	c.TrxReport()
	return ok
}

// InsLockAndUndo is a placeholder for lock/undo handling.
func (c *Cur) InsLockAndUndo() {
}

// TrxReport is a placeholder for transaction reporting.
func (c *Cur) TrxReport() {
}
