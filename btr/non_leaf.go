package btr

// InsertOnNonLeafLevel inserts a key and right child into the parent of left.
func InsertOnNonLeafLevel(t *Tree, left *node, key []byte, right *node) {
	if t == nil || left == nil || right == nil {
		return
	}
	t.insertIntoParent(left, key, right)
}

// AttachHalfPages links leaf siblings after a split.
func AttachHalfPages(left, right *node) {
	if left == nil || right == nil {
		return
	}
	right.prev = left
	right.next = left.next
	if left.next != nil {
		left.next.prev = right
	}
	left.next = right
}

// NodePtrDelete removes a child pointer from its parent.
func NodePtrDelete(child *node) {
	if child == nil || child.parent == nil {
		return
	}
	parent := child.parent
	idx := child.indexInParent()
	if idx < 0 {
		return
	}
	if idx > 0 && idx-1 < len(parent.keys) {
		parent.keys = removeBytes(parent.keys, idx-1)
	} else if len(parent.keys) > 0 {
		parent.keys = removeBytes(parent.keys, 0)
	}
	parent.children = removeNode(parent.children, idx)
	child.parent = nil
}

// LiftPageUp promotes a separator key into the parent.
func LiftPageUp(t *Tree, left *node, key []byte, right *node) {
	InsertOnNonLeafLevel(t, left, key, right)
}
