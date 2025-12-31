package btr

import (
	"bytes"
	"sort"
)

// CompareFunc compares two keys, returning <0, 0, >0 for a<b, a==b, a>b.
type CompareFunc func(a, b []byte) int

// Record holds a key/value pair stored in the tree.
type Record struct {
	Key   []byte
	Value []byte
}

// Tree is an in-memory B+ tree used as a stand-in for the on-disk variant.
type Tree struct {
	root     *node
	order    int
	compare  CompareFunc
	size     int
	modCount uint64
	deleted  map[string]struct{}
}

type node struct {
	leaf     bool
	keys     [][]byte
	values   [][]byte
	children []*node
	parent   *node
	next     *node
	prev     *node
}

// NewTree builds a B+ tree with the requested order.
func NewTree(order int, compare CompareFunc) *Tree {
	if order < 3 {
		order = 3
	}
	if compare == nil {
		compare = bytes.Compare
	}
	return &Tree{
		order:   order,
		compare: compare,
	}
}

// Size returns the number of records in the tree.
func (t *Tree) Size() int {
	if t == nil {
		return 0
	}
	return t.size
}

// Search looks for the key and returns the stored value.
func (t *Tree) Search(key []byte) ([]byte, bool) {
	if t == nil || t.root == nil {
		return nil, false
	}
	if t.isDeleted(key) {
		return nil, false
	}
	leaf := t.findLeaf(key)
	if leaf == nil {
		return nil, false
	}
	idx := t.keyIndex(leaf.keys, key)
	if idx < len(leaf.keys) && t.compare(leaf.keys[idx], key) == 0 {
		return cloneBytes(leaf.values[idx]), true
	}
	return nil, false
}

// Insert inserts a key/value pair, returning true if it replaced an existing key.
func (t *Tree) Insert(key, value []byte) bool {
	if t.root == nil {
		t.root = &node{
			leaf:   true,
			keys:   [][]byte{cloneBytes(key)},
			values: [][]byte{cloneBytes(value)},
		}
		t.size = 1
		t.modCount++
		if t.deleted != nil {
			delete(t.deleted, string(key))
		}
		return false
	}

	if t.deleted != nil {
		delete(t.deleted, string(key))
	}

	leaf := t.findLeaf(key)
	idx := t.keyIndex(leaf.keys, key)
	if idx < len(leaf.keys) && t.compare(leaf.keys[idx], key) == 0 {
		leaf.values[idx] = cloneBytes(value)
		t.modCount++
		return true
	}

	leaf.keys = insertBytes(leaf.keys, idx, cloneBytes(key))
	leaf.values = insertBytes(leaf.values, idx, cloneBytes(value))
	t.size++
	t.modCount++

	if len(leaf.keys) > t.maxKeys() {
		t.splitLeaf(leaf)
	} else if idx == 0 {
		t.updateParentKey(leaf)
	}
	return false
}

// Delete removes the key and returns true if it was found.
func (t *Tree) Delete(key []byte) bool {
	if t == nil || t.root == nil {
		return false
	}
	if t.deleted != nil {
		delete(t.deleted, string(key))
	}
	leaf := t.findLeaf(key)
	if leaf == nil {
		return false
	}
	idx := t.keyIndex(leaf.keys, key)
	if idx >= len(leaf.keys) || t.compare(leaf.keys[idx], key) != 0 {
		return false
	}

	leaf.keys = removeBytes(leaf.keys, idx)
	leaf.values = removeBytes(leaf.values, idx)
	t.size--
	t.modCount++

	if leaf == t.root {
		if len(leaf.keys) == 0 {
			t.root = nil
		}
		return true
	}

	if idx == 0 && len(leaf.keys) > 0 {
		t.updateParentKey(leaf)
	}
	t.rebalanceAfterDelete(leaf)
	return true
}

func (t *Tree) maxKeys() int {
	return t.order - 1
}

func (t *Tree) minChildren() int {
	return (t.order + 1) / 2
}

func (t *Tree) minKeysInternal() int {
	return t.minChildren() - 1
}

func (t *Tree) minKeysLeaf() int {
	return (t.maxKeys() + 1) / 2
}

func (t *Tree) findLeaf(key []byte) *node {
	n := t.root
	for n != nil && !n.leaf {
		idx := t.childIndex(n.keys, key)
		n = n.children[idx]
	}
	return n
}

func (t *Tree) keyIndex(keys [][]byte, key []byte) int {
	return sort.Search(len(keys), func(i int) bool {
		return t.compare(keys[i], key) >= 0
	})
}

func (t *Tree) childIndex(keys [][]byte, key []byte) int {
	return sort.Search(len(keys), func(i int) bool {
		return t.compare(keys[i], key) > 0
	})
}

func (t *Tree) splitLeaf(leaf *node) {
	mid := len(leaf.keys) / 2
	right := &node{
		leaf:   true,
		keys:   append([][]byte(nil), leaf.keys[mid:]...),
		values: append([][]byte(nil), leaf.values[mid:]...),
		parent: leaf.parent,
		next:   leaf.next,
		prev:   leaf,
	}
	leaf.keys = leaf.keys[:mid]
	leaf.values = leaf.values[:mid]

	if leaf.next != nil {
		leaf.next.prev = right
	}
	leaf.next = right

	promote := right.keys[0]
	t.insertIntoParent(leaf, promote, right)
}

func (t *Tree) insertIntoParent(left *node, key []byte, right *node) {
	parent := left.parent
	if parent == nil {
		t.root = &node{
			leaf:     false,
			keys:     [][]byte{cloneBytes(key)},
			children: []*node{left, right},
		}
		left.parent = t.root
		right.parent = t.root
		return
	}

	idx := left.indexInParent()
	parent.keys = insertBytes(parent.keys, idx, cloneBytes(key))
	parent.children = insertNode(parent.children, idx+1, right)
	right.parent = parent

	if len(parent.keys) > t.maxKeys() {
		t.splitInternal(parent)
	}
}

func (t *Tree) splitInternal(n *node) {
	mid := len(n.keys) / 2
	promote := n.keys[mid]

	right := &node{
		leaf:     false,
		keys:     append([][]byte(nil), n.keys[mid+1:]...),
		children: append([]*node(nil), n.children[mid+1:]...),
		parent:   n.parent,
	}
	for _, child := range right.children {
		child.parent = right
	}

	n.keys = n.keys[:mid]
	n.children = n.children[:mid+1]

	t.insertIntoParent(n, promote, right)
}

func (t *Tree) updateParentKey(child *node) {
	if child == nil || child.parent == nil || len(child.keys) == 0 {
		return
	}
	idx := child.indexInParent()
	if idx > 0 {
		child.parent.keys[idx-1] = child.keys[0]
	}
}

func (t *Tree) rebalanceAfterDelete(n *node) {
	for n != nil {
		if n == t.root {
			if !n.leaf && len(n.keys) == 0 && len(n.children) > 0 {
				t.root = n.children[0]
				t.root.parent = nil
			} else if n.leaf && len(n.keys) == 0 {
				t.root = nil
			}
			return
		}

		if n.leaf {
			if len(n.keys) >= t.minKeysLeaf() {
				return
			}
			if t.borrowFromLeftLeaf(n) || t.borrowFromRightLeaf(n) {
				return
			}
			n = t.mergeLeaf(n)
			continue
		}

		if len(n.keys) >= t.minKeysInternal() {
			return
		}
		if t.borrowFromLeftInternal(n) || t.borrowFromRightInternal(n) {
			return
		}
		n = t.mergeInternal(n)
	}
}

func (t *Tree) borrowFromLeftLeaf(n *node) bool {
	parent := n.parent
	if parent == nil {
		return false
	}
	idx := n.indexInParent()
	if idx <= 0 {
		return false
	}
	left := parent.children[idx-1]
	if len(left.keys) <= t.minKeysLeaf() {
		return false
	}

	k := left.keys[len(left.keys)-1]
	v := left.values[len(left.values)-1]
	left.keys = left.keys[:len(left.keys)-1]
	left.values = left.values[:len(left.values)-1]

	n.keys = insertBytes(n.keys, 0, k)
	n.values = insertBytes(n.values, 0, v)
	parent.keys[idx-1] = n.keys[0]
	return true
}

func (t *Tree) borrowFromRightLeaf(n *node) bool {
	parent := n.parent
	if parent == nil {
		return false
	}
	idx := n.indexInParent()
	if idx < 0 || idx+1 >= len(parent.children) {
		return false
	}
	right := parent.children[idx+1]
	if len(right.keys) <= t.minKeysLeaf() {
		return false
	}

	k := right.keys[0]
	v := right.values[0]
	right.keys = removeBytes(right.keys, 0)
	right.values = removeBytes(right.values, 0)

	n.keys = append(n.keys, k)
	n.values = append(n.values, v)
	parent.keys[idx] = right.keys[0]
	return true
}

func (t *Tree) mergeLeaf(n *node) *node {
	parent := n.parent
	if parent == nil {
		return nil
	}
	idx := n.indexInParent()
	if idx > 0 {
		left := parent.children[idx-1]
		left.keys = append(left.keys, n.keys...)
		left.values = append(left.values, n.values...)
		left.next = n.next
		if n.next != nil {
			n.next.prev = left
		}
		parent.keys = removeBytes(parent.keys, idx-1)
		parent.children = removeNode(parent.children, idx)
		return parent
	}
	if idx+1 < len(parent.children) {
		right := parent.children[idx+1]
		n.keys = append(n.keys, right.keys...)
		n.values = append(n.values, right.values...)
		n.next = right.next
		if right.next != nil {
			right.next.prev = n
		}
		parent.keys = removeBytes(parent.keys, idx)
		parent.children = removeNode(parent.children, idx+1)
		return parent
	}
	return parent
}

func (t *Tree) borrowFromLeftInternal(n *node) bool {
	parent := n.parent
	if parent == nil {
		return false
	}
	idx := n.indexInParent()
	if idx <= 0 {
		return false
	}
	left := parent.children[idx-1]
	if len(left.keys) <= t.minKeysInternal() {
		return false
	}

	sep := parent.keys[idx-1]
	borrowKey := left.keys[len(left.keys)-1]
	borrowChild := left.children[len(left.children)-1]
	left.keys = left.keys[:len(left.keys)-1]
	left.children = left.children[:len(left.children)-1]

	n.keys = insertBytes(n.keys, 0, sep)
	n.children = insertNode(n.children, 0, borrowChild)
	borrowChild.parent = n
	parent.keys[idx-1] = borrowKey
	return true
}

func (t *Tree) borrowFromRightInternal(n *node) bool {
	parent := n.parent
	if parent == nil {
		return false
	}
	idx := n.indexInParent()
	if idx < 0 || idx+1 >= len(parent.children) {
		return false
	}
	right := parent.children[idx+1]
	if len(right.keys) <= t.minKeysInternal() {
		return false
	}

	sep := parent.keys[idx]
	borrowKey := right.keys[0]
	borrowChild := right.children[0]
	right.keys = removeBytes(right.keys, 0)
	right.children = removeNode(right.children, 0)

	n.keys = append(n.keys, sep)
	n.children = append(n.children, borrowChild)
	borrowChild.parent = n
	parent.keys[idx] = borrowKey
	return true
}

func (t *Tree) mergeInternal(n *node) *node {
	parent := n.parent
	if parent == nil {
		return nil
	}
	idx := n.indexInParent()
	if idx > 0 {
		left := parent.children[idx-1]
		sep := parent.keys[idx-1]
		left.keys = append(left.keys, sep)
		left.keys = append(left.keys, n.keys...)
		left.children = append(left.children, n.children...)
		for _, child := range n.children {
			child.parent = left
		}
		parent.keys = removeBytes(parent.keys, idx-1)
		parent.children = removeNode(parent.children, idx)
		return parent
	}
	if idx+1 < len(parent.children) {
		right := parent.children[idx+1]
		sep := parent.keys[idx]
		n.keys = append(n.keys, sep)
		n.keys = append(n.keys, right.keys...)
		n.children = append(n.children, right.children...)
		for _, child := range right.children {
			child.parent = n
		}
		parent.keys = removeBytes(parent.keys, idx)
		parent.children = removeNode(parent.children, idx+1)
		return parent
	}
	return parent
}

func (n *node) indexInParent() int {
	if n == nil || n.parent == nil {
		return -1
	}
	for i, child := range n.parent.children {
		if child == n {
			return i
		}
	}
	return -1
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func insertBytes(slice [][]byte, idx int, val []byte) [][]byte {
	if idx < 0 {
		idx = 0
	}
	if idx > len(slice) {
		idx = len(slice)
	}
	slice = append(slice, nil)
	copy(slice[idx+1:], slice[idx:])
	slice[idx] = val
	return slice
}

func insertNode(slice []*node, idx int, val *node) []*node {
	if idx < 0 {
		idx = 0
	}
	if idx > len(slice) {
		idx = len(slice)
	}
	slice = append(slice, nil)
	copy(slice[idx+1:], slice[idx:])
	slice[idx] = val
	return slice
}

func removeBytes(slice [][]byte, idx int) [][]byte {
	if idx < 0 || idx >= len(slice) {
		return slice
	}
	copy(slice[idx:], slice[idx+1:])
	slice[len(slice)-1] = nil
	return slice[:len(slice)-1]
}

func removeNode(slice []*node, idx int) []*node {
	if idx < 0 || idx >= len(slice) {
		return slice
	}
	copy(slice[idx:], slice[idx+1:])
	slice[len(slice)-1] = nil
	return slice[:len(slice)-1]
}
