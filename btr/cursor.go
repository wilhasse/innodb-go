package btr

// Cursor positions on a leaf record in the tree.
type Cursor struct {
	node  *node
	index int
}

// Valid reports whether the cursor points to a record.
func (c *Cursor) Valid() bool {
	return c != nil && c.node != nil && c.index >= 0 && c.index < len(c.node.keys)
}

// Key returns the current key.
func (c *Cursor) Key() []byte {
	if !c.Valid() {
		return nil
	}
	return cloneBytes(c.node.keys[c.index])
}

// Value returns the current value.
func (c *Cursor) Value() []byte {
	if !c.Valid() {
		return nil
	}
	if len(c.node.values) == 0 {
		return nil
	}
	return cloneBytes(c.node.values[c.index])
}

// Next advances to the next record.
func (c *Cursor) Next() bool {
	if c == nil || c.node == nil {
		return false
	}
	if c.index+1 < len(c.node.keys) {
		c.index++
		return true
	}
	if c.node.next == nil {
		c.node = nil
		c.index = 0
		return false
	}
	c.node = c.node.next
	c.index = 0
	return c.Valid()
}

// Prev moves to the previous record.
func (c *Cursor) Prev() bool {
	if c == nil || c.node == nil {
		return false
	}
	if c.index > 0 {
		c.index--
		return true
	}
	if c.node.prev == nil {
		c.node = nil
		c.index = 0
		return false
	}
	c.node = c.node.prev
	c.index = len(c.node.keys) - 1
	return c.Valid()
}

// First returns a cursor positioned at the first record.
func (t *Tree) First() *Cursor {
	if t == nil || t.root == nil {
		return nil
	}
	n := t.root
	for n != nil && !n.leaf {
		n = n.children[0]
	}
	if n == nil || len(n.keys) == 0 {
		return nil
	}
	return &Cursor{node: n, index: 0}
}

// Last returns a cursor positioned at the last record.
func (t *Tree) Last() *Cursor {
	if t == nil || t.root == nil {
		return nil
	}
	n := t.root
	for n != nil && !n.leaf {
		n = n.children[len(n.children)-1]
	}
	if n == nil || len(n.keys) == 0 {
		return nil
	}
	return &Cursor{node: n, index: len(n.keys) - 1}
}

// Seek returns a cursor positioned at the first key >= the given key.
func (t *Tree) Seek(key []byte) *Cursor {
	if t == nil || t.root == nil {
		return nil
	}
	leaf := t.findLeaf(key)
	if leaf == nil || len(leaf.keys) == 0 {
		return nil
	}
	idx := t.keyIndex(leaf.keys, key)
	if idx >= len(leaf.keys) {
		if leaf.next == nil {
			return nil
		}
		return &Cursor{node: leaf.next, index: 0}
	}
	return &Cursor{node: leaf, index: idx}
}
