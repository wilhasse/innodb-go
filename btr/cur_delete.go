package btr

// RecSetDeletedFlag marks or unmarks the current record.
func (c *Cur) RecSetDeletedFlag(deleted bool) bool {
	if c == nil || c.Tree == nil || !c.Valid() {
		return false
	}
	key := c.Key()
	if deleted {
		return c.Tree.markDeleted(key)
	}
	return c.Tree.unmarkDeleted(key)
}

// DelMarkSetClustRec marks the current clustered record as deleted.
func (c *Cur) DelMarkSetClustRec() bool {
	return c.RecSetDeletedFlag(true)
}

// DelMarkSetSecRec marks the current secondary record as deleted.
func (c *Cur) DelMarkSetSecRec() bool {
	return c.RecSetDeletedFlag(true)
}

// DelUnmarkForIbuf clears a delete mark for ibuf operations.
func (c *Cur) DelUnmarkForIbuf() bool {
	return c.RecSetDeletedFlag(false)
}

// OptimisticDelete removes the current record without heavy latching.
func (c *Cur) OptimisticDelete() bool {
	if c == nil || c.Tree == nil || !c.Valid() {
		return false
	}
	key := c.Key()
	c.Tree.unmarkDeleted(key)
	ok := c.Tree.Delete(key)
	c.Cursor = nil
	return ok
}

// PessimisticDelete removes the current record with full latching (stubbed).
func (c *Cur) PessimisticDelete() bool {
	return c.OptimisticDelete()
}
