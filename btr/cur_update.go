package btr

// UpdateAllocZip is a stub for compressed page allocation during update.
func (c *Cur) UpdateAllocZip() {
}

// ParseUpdateInPlace attempts to update the record in place.
func (c *Cur) ParseUpdateInPlace(value []byte) bool {
	return c.UpdateInPlace(value)
}

// UpdateInPlace updates a value without changing its size.
func (c *Cur) UpdateInPlace(value []byte) bool {
	if c == nil || !c.Valid() {
		return false
	}
	if len(c.Cursor.node.values) == 0 {
		return false
	}
	curVal := c.Cursor.node.values[c.Cursor.index]
	if len(curVal) != len(value) {
		return false
	}
	c.Cursor.node.values[c.Cursor.index] = cloneBytes(value)
	if c.Tree != nil {
		c.Tree.modCount++
	}
	return true
}

// OptimisticUpdate tries to update without changing record size.
func (c *Cur) OptimisticUpdate(value []byte) bool {
	if c == nil {
		return false
	}
	return c.UpdateInPlace(value)
}

// PessimisticUpdate updates even if the record size changes.
func (c *Cur) PessimisticUpdate(value []byte) bool {
	if c == nil || !c.Valid() || c.Tree == nil {
		return false
	}
	key := c.Cursor.node.keys[c.Cursor.index]
	c.Tree.Insert(key, value)
	c.Cursor = c.Tree.Seek(key)
	return true
}
