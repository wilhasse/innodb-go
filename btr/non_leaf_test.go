package btr

import "testing"

func TestNonLeafInsertNodePointers(t *testing.T) {
	tree := NewTree(3, nil)
	for _, key := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		tree.Insert([]byte(key), []byte("v"+key))
	}
	if treeHeight(tree) < 2 {
		t.Fatalf("expected tree height >= 2, got %d", treeHeight(tree))
	}
	if !validateNodePointers(tree.root, tree.compare) {
		t.Fatalf("node pointer validation failed")
	}
}

func validateNodePointers(n *node, cmp CompareFunc) bool {
	if n == nil || n.leaf {
		return true
	}
	if len(n.children) != len(n.keys)+1 {
		return false
	}
	for i := 0; i < len(n.keys); i++ {
		left := n.children[i]
		right := n.children[i+1]
		leftMax := maxKey(left)
		rightMin := minKey(right)
		if leftMax == nil || rightMin == nil {
			return false
		}
		if cmp(leftMax, n.keys[i]) > 0 {
			return false
		}
		if cmp(rightMin, n.keys[i]) < 0 {
			return false
		}
	}
	for _, child := range n.children {
		if !validateNodePointers(child, cmp) {
			return false
		}
	}
	return true
}

func minKey(n *node) []byte {
	if n == nil {
		return nil
	}
	for n != nil && !n.leaf {
		if len(n.children) == 0 {
			break
		}
		n = n.children[0]
	}
	if n == nil || len(n.keys) == 0 {
		return nil
	}
	return n.keys[0]
}

func maxKey(n *node) []byte {
	if n == nil {
		return nil
	}
	for n != nil && !n.leaf {
		if len(n.children) == 0 {
			break
		}
		n = n.children[len(n.children)-1]
	}
	if n == nil || len(n.keys) == 0 {
		return nil
	}
	return n.keys[len(n.keys)-1]
}
