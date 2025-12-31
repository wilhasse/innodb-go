package btr

// EstimateNRowsInRange counts visible records between low and high (inclusive).
func EstimateNRowsInRange(t *Tree, low, high []byte) int {
	if t == nil || t.root == nil {
		return 0
	}
	if t.compare(low, high) > 0 {
		return 0
	}
	cur := t.Seek(low)
	if cur == nil {
		return 0
	}
	count := 0
	for cur.Valid() {
		key := cur.Key()
		if t.compare(key, high) > 0 {
			break
		}
		if !t.isDeleted(key) {
			count++
		}
		if !cur.Next() {
			break
		}
	}
	return count
}

// EstimateNumberOfDifferentKeyVals counts visible distinct keys in the tree.
func EstimateNumberOfDifferentKeyVals(t *Tree) int {
	if t == nil || t.root == nil {
		return 0
	}
	cur := t.First()
	if cur == nil {
		return 0
	}
	count := 0
	for cur.Valid() {
		key := cur.Key()
		if !t.isDeleted(key) {
			count++
		}
		if !cur.Next() {
			break
		}
	}
	return count
}
