package eval

// ifStep executes a single step of an if-statement node.
func ifStep(thr *Thr) *Thr {
	if thr == nil {
		return thr
	}
	node, ok := thr.RunNode.(*IfNode)
	if !ok || node == nil {
		return thr
	}

	if thr.PrevNode == NodeGetParent(node) {
		EvalExp(node.Cond)

		if EvalNodeGetIBoolVal(node.Cond) {
			thr.RunNode = node.StatList
		} else if node.ElsePart != nil {
			thr.RunNode = node.ElsePart
		} else if node.ElsifList != nil {
			for elsif := node.ElsifList; elsif != nil; {
				EvalExp(elsif.Cond)
				if EvalNodeGetIBoolVal(elsif.Cond) {
					thr.RunNode = elsif.StatList
					break
				}
				next := NodeGetNext(elsif)
				nextElsif, _ := next.(*ElsifNode)
				if nextElsif == nil {
					thr.RunNode = nil
					break
				}
				elsif = nextElsif
			}
		} else {
			thr.RunNode = nil
		}
	} else {
		thr.RunNode = nil
	}

	if thr.RunNode == nil {
		thr.RunNode = NodeGetParent(node)
	}

	return thr
}

// whileStep executes a single step of a while-statement node.
func whileStep(thr *Thr) *Thr {
	if thr == nil {
		return thr
	}
	node, ok := thr.RunNode.(*WhileNode)
	if !ok || node == nil {
		return thr
	}

	EvalExp(node.Cond)

	if EvalNodeGetIBoolVal(node.Cond) {
		thr.RunNode = node.StatList
	} else {
		thr.RunNode = NodeGetParent(node)
	}

	return thr
}

// assignStep executes a single step of an assignment node.
func assignStep(thr *Thr) *Thr {
	if thr == nil {
		return thr
	}
	node, ok := thr.RunNode.(*AssignNode)
	if !ok || node == nil {
		return thr
	}

	EvalExp(node.Val)

	if node.Var != nil {
		if node.Var.Alias != nil {
			EvalNodeCopyVal(node.Var.Alias, node.Val)
		} else {
			EvalNodeCopyVal(node.Var, node.Val)
		}
	}

	thr.RunNode = NodeGetParent(node)

	return thr
}

// forStep executes a single step of a for-loop node.
func forStep(thr *Thr) *Thr {
	if thr == nil {
		return thr
	}
	node, ok := thr.RunNode.(*ForNode)
	if !ok || node == nil {
		return thr
	}

	parent := NodeGetParent(node)
	var loopVarValue int64

	if thr.PrevNode != parent {
		thr.RunNode = NodeGetNext(thr.PrevNode)
		if thr.RunNode != nil {
			return thr
		}
		loopVarValue = EvalNodeGetIntVal(node.LoopVar) + 1
	} else {
		EvalExp(node.LoopStartLimit)
		EvalExp(node.LoopEndLimit)
		loopVarValue = EvalNodeGetIntVal(node.LoopStartLimit)
		node.LoopEndValue = EvalNodeGetIntVal(node.LoopEndLimit)
	}

	if loopVarValue > node.LoopEndValue {
		thr.RunNode = parent
	} else {
		EvalNodeSetIntVal(node.LoopVar, loopVarValue)
		thr.RunNode = node.StatList
	}

	return thr
}

// exitStep executes a single step of an exit-statement node.
func exitStep(thr *Thr) *Thr {
	if thr == nil {
		return thr
	}
	node, ok := thr.RunNode.(*ExitNode)
	if !ok || node == nil {
		return thr
	}

	loopNode := NodeGetContainingLoop(node)
	if loopNode == nil {
		return thr
	}
	thr.RunNode = NodeGetParent(loopNode)

	return thr
}

// returnStep executes a single step of a return-statement node.
func returnStep(thr *Thr) *Thr {
	if thr == nil {
		return thr
	}
	node, ok := thr.RunNode.(*ReturnNode)
	if !ok || node == nil {
		return thr
	}

	parent := NodeRef(node)
	for parent != nil && NodeTypeOf(parent) != NodeProc {
		parent = NodeGetParent(parent)
	}
	if parent == nil {
		return thr
	}

	thr.RunNode = NodeGetParent(parent)

	return thr
}
