package btr

func (t *Tree) markDeleted(key []byte) bool {
	if t == nil {
		return false
	}
	if _, _, ok := t.findKey(key); !ok {
		return false
	}
	if t.deleted == nil {
		t.deleted = make(map[string]struct{})
	}
	t.deleted[string(key)] = struct{}{}
	t.modCount++
	return true
}

func (t *Tree) unmarkDeleted(key []byte) bool {
	if t == nil || t.deleted == nil {
		return false
	}
	if _, ok := t.deleted[string(key)]; !ok {
		return false
	}
	delete(t.deleted, string(key))
	t.modCount++
	return true
}

func (t *Tree) isDeleted(key []byte) bool {
	if t == nil || t.deleted == nil {
		return false
	}
	_, ok := t.deleted[string(key)]
	return ok
}

func (t *Tree) findKey(key []byte) (*node, int, bool) {
	if t == nil || t.root == nil {
		return nil, 0, false
	}
	leaf := t.findLeaf(key)
	if leaf == nil {
		return nil, 0, false
	}
	idx := t.keyIndex(leaf.keys, key)
	if idx < len(leaf.keys) && t.compare(leaf.keys[idx], key) == 0 {
		return leaf, idx, true
	}
	return nil, 0, false
}
