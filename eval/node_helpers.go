package eval

// NodeTypeOf returns the node type or NodeUnknown when nil.
func NodeTypeOf(node NodeRef) NodeType {
	base := nodeBase(node)
	if base == nil {
		return NodeUnknown
	}
	return base.Type
}

// NodeGetParent returns the node parent.
func NodeGetParent(node NodeRef) NodeRef {
	base := nodeBase(node)
	if base == nil {
		return nil
	}
	return base.Parent
}

// NodeSetParent updates the node parent.
func NodeSetParent(node, parent NodeRef) {
	base := nodeBase(node)
	if base == nil {
		return
	}
	base.Parent = parent
}

// NodeGetNext returns the next node in a list.
func NodeGetNext(node NodeRef) NodeRef {
	base := nodeBase(node)
	if base == nil {
		return nil
	}
	return base.Next
}

// NodeSetNext updates the next node in a list.
func NodeSetNext(node, next NodeRef) {
	base := nodeBase(node)
	if base == nil {
		return
	}
	base.Next = next
}

// NodeGetContainingLoop returns the first parent loop node.
func NodeGetContainingLoop(node NodeRef) NodeRef {
	for parent := NodeGetParent(node); parent != nil; parent = NodeGetParent(parent) {
		switch NodeTypeOf(parent) {
		case NodeWhile, NodeFor:
			return parent
		}
	}
	return nil
}

// EvalSym copies the aliased value to the symbol when available.
func EvalSym(sym *Symbol) {
	if sym == nil || sym.Alias == nil {
		return
	}
	EvalNodeCopyVal(sym, sym.Alias)
}

// EvalExp evaluates a node expression.
func EvalExp(node NodeRef) {
	switch n := node.(type) {
	case *Symbol:
		EvalSym(n)
	}
}

// EvalNodeSetIntVal assigns an integer value to the node.
func EvalNodeSetIntVal(node NodeRef, val int64) {
	base := nodeBase(node)
	if base == nil {
		return
	}
	base.Val.Kind = KindInt
	base.Val.Int = val
}

// EvalNodeGetIntVal reads an integer value from the node.
func EvalNodeGetIntVal(node NodeRef) int64 {
	base := nodeBase(node)
	if base == nil {
		return 0
	}
	if base.Val.Kind == KindInt {
		return base.Val.Int
	}
	if base.Val.Kind == KindBool {
		if base.Val.Bool {
			return 1
		}
		return 0
	}
	return 0
}

// EvalNodeGetIBoolVal reads a boolean value from the node.
func EvalNodeGetIBoolVal(node NodeRef) bool {
	base := nodeBase(node)
	if base == nil {
		return false
	}
	switch base.Val.Kind {
	case KindBool:
		return base.Val.Bool
	case KindInt:
		return base.Val.Int != 0
	default:
		return false
	}
}

// EvalNodeCopyVal copies a value between nodes.
func EvalNodeCopyVal(dst, src NodeRef) {
	dstBase := nodeBase(dst)
	srcBase := nodeBase(src)
	if dstBase == nil || srcBase == nil {
		return
	}
	dstBase.Val = srcBase.Val
	if srcBase.Val.Kind == KindBytes && srcBase.Val.Bytes != nil {
		dstBase.Val.Bytes = append([]byte(nil), srcBase.Val.Bytes...)
	}
}
