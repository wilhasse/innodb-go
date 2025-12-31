package btr

import "github.com/wilhasse/innodb-go/ut"

// SearchToNthLevel searches the tree and positions the cursor at the requested level.
func (c *Cur) SearchToNthLevel(key []byte, mode SearchMode, level int) bool {
	if c == nil || c.Tree == nil || c.Tree.root == nil {
		return false
	}
	if level < 0 {
		level = 0
	}
	c.Path = c.Path[:0]

	height := treeHeight(c.Tree)
	c.TreeHeight = ut.Ulint(height)
	if level > height {
		level = height
	}
	targetDepth := height - level

	n := c.Tree.root
	depth := 0
	for {
		if n == nil {
			c.Invalidate()
			return false
		}
		if n.leaf || depth == targetDepth {
			idx := c.Tree.keyIndex(n.keys, key)
			switch mode {
			case SearchLE:
				if idx >= len(n.keys) {
					idx = len(n.keys) - 1
				} else if len(n.keys) > 0 && c.Tree.compare(n.keys[idx], key) > 0 {
					idx--
				}
				if idx < 0 {
					if n.leaf && n.prev != nil {
						n = n.prev
						idx = len(n.keys) - 1
					} else {
						c.Invalidate()
						return false
					}
				}
			default:
				if idx >= len(n.keys) {
					if n.leaf && n.next != nil {
						n = n.next
						idx = 0
					} else {
						c.Invalidate()
						return false
					}
				}
			}
			c.addPathInfo(ut.Ulint(idx), ut.Ulint(len(n.keys)))
			c.Cursor = &Cursor{node: n, index: idx}
			c.Flag = CurBinary
			return c.Valid()
		}

		childIdx := c.Tree.childIndex(n.keys, key)
		c.addPathInfo(ut.Ulint(childIdx), ut.Ulint(len(n.keys)))
		if childIdx >= len(n.children) {
			childIdx = len(n.children) - 1
		}
		n = n.children[childIdx]
		depth++
	}
}

// LatchLeaves is a no-op placeholder for leaf latching.
func (c *Cur) LatchLeaves() {
}

func (c *Cur) addPathInfo(nthRec, nRecs ut.Ulint) {
	c.Path = append(c.Path, PathSlot{NthRec: nthRec, NRecs: nRecs})
}

func treeHeight(t *Tree) int {
	if t == nil || t.root == nil {
		return 0
	}
	height := 0
	n := t.root
	for n != nil && !n.leaf {
		height++
		if len(n.children) == 0 {
			break
		}
		n = n.children[0]
	}
	return height
}
