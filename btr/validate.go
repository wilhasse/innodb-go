package btr

import (
	"errors"
	"fmt"
)

// CheckNodePtr validates separator ordering for an internal node.
func CheckNodePtr(n *node, cmp CompareFunc) bool {
	if n == nil || n.leaf {
		return true
	}
	if len(n.children) != len(n.keys)+1 {
		return false
	}
	for i := 0; i < len(n.keys); i++ {
		left := n.children[i]
		right := n.children[i+1]
		leftMax := maxNodeKey(left, false)
		rightMin := minNodeKey(right, false)
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
	return true
}

// ValidateIndex checks basic B+ tree invariants.
func ValidateIndex(t *Tree) error {
	if t == nil || t.root == nil {
		return nil
	}
	if err := validateNode(t.root, t.compare); err != nil {
		return err
	}
	if err := validateLeafLinks(t.root, t.compare); err != nil {
		return err
	}
	return nil
}

// PrintSize returns a summary of node/leaf counts.
func PrintSize(t *Tree) string {
	if t == nil || t.root == nil {
		return "nodes=0 keys=0 leaves=0"
	}
	nodes, keys, leaves := countNodes(t.root)
	return fmt.Sprintf("nodes=%d keys=%d leaves=%d", nodes, keys, leaves)
}

func validateNode(n *node, cmp CompareFunc) error {
	if n == nil {
		return nil
	}
	for i := 1; i < len(n.keys); i++ {
		if cmp(n.keys[i-1], n.keys[i]) >= 0 {
			return errors.New("btr: keys not ordered")
		}
	}
	if n.leaf {
		if len(n.keys) != len(n.values) {
			return errors.New("btr: leaf values mismatch")
		}
		return nil
	}
	if len(n.children) != len(n.keys)+1 {
		return errors.New("btr: child count mismatch")
	}
	if !CheckNodePtr(n, cmp) {
		return errors.New("btr: node pointer order mismatch")
	}
	for _, child := range n.children {
		if err := validateNode(child, cmp); err != nil {
			return err
		}
	}
	return nil
}

func validateLeafLinks(root *node, cmp CompareFunc) error {
	left := root
	for left != nil && !left.leaf {
		if len(left.children) == 0 {
			break
		}
		left = left.children[0]
	}
	if left == nil {
		return nil
	}
	prevKey := []byte(nil)
	for n := left; n != nil; n = n.next {
		if n.prev != nil && n.prev.next != n {
			return errors.New("btr: leaf prev/next mismatch")
		}
		for _, key := range n.keys {
			if prevKey != nil && cmp(prevKey, key) >= 0 {
				return errors.New("btr: leaf order mismatch")
			}
			prevKey = key
		}
	}
	return nil
}

func countNodes(n *node) (nodes, keys, leaves int) {
	if n == nil {
		return 0, 0, 0
	}
	nodes = 1
	keys = len(n.keys)
	if n.leaf {
		leaves = 1
		return
	}
	for _, child := range n.children {
		cn, ck, cl := countNodes(child)
		nodes += cn
		keys += ck
		leaves += cl
	}
	return
}

func minNodeKey(n *node, includeInternal bool) []byte {
	if n == nil {
		return nil
	}
	if n.leaf {
		if len(n.keys) == 0 {
			return nil
		}
		return n.keys[0]
	}
	if includeInternal && len(n.keys) > 0 {
		return n.keys[0]
	}
	if len(n.children) == 0 {
		return nil
	}
	return minNodeKey(n.children[0], includeInternal)
}

func maxNodeKey(n *node, includeInternal bool) []byte {
	if n == nil {
		return nil
	}
	if n.leaf {
		if len(n.keys) == 0 {
			return nil
		}
		return n.keys[len(n.keys)-1]
	}
	if includeInternal && len(n.keys) > 0 {
		return n.keys[len(n.keys)-1]
	}
	if len(n.children) == 0 {
		return nil
	}
	return maxNodeKey(n.children[len(n.children)-1], includeInternal)
}
