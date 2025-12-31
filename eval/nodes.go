package eval

// Thr tracks query graph execution state.
type Thr struct {
	RunNode  NodeRef
	PrevNode NodeRef
}

// Symbol represents a variable or literal node.
type Symbol struct {
	Node
	Alias NodeRef
}

// Base returns the embedded node data.
func (s *Symbol) Base() *Node {
	if s == nil {
		return nil
	}
	return &s.Node
}

// ElsifNode represents an elsif branch.
type ElsifNode struct {
	Node
	Cond     NodeRef
	StatList NodeRef
}

// Base returns the embedded node data.
func (n *ElsifNode) Base() *Node {
	if n == nil {
		return nil
	}
	return &n.Node
}

// IfNode represents an if-statement node.
type IfNode struct {
	Node
	Cond      NodeRef
	StatList  NodeRef
	ElsePart  NodeRef
	ElsifList *ElsifNode
}

// Base returns the embedded node data.
func (n *IfNode) Base() *Node {
	if n == nil {
		return nil
	}
	return &n.Node
}

// WhileNode represents a while-statement node.
type WhileNode struct {
	Node
	Cond     NodeRef
	StatList NodeRef
}

// Base returns the embedded node data.
func (n *WhileNode) Base() *Node {
	if n == nil {
		return nil
	}
	return &n.Node
}

// ForNode represents a for-loop node.
type ForNode struct {
	Node
	LoopVar        *Symbol
	LoopStartLimit NodeRef
	LoopEndLimit   NodeRef
	LoopEndValue   int64
	StatList       NodeRef
}

// Base returns the embedded node data.
func (n *ForNode) Base() *Node {
	if n == nil {
		return nil
	}
	return &n.Node
}

// ExitNode represents an exit-statement node.
type ExitNode struct {
	Node
}

// Base returns the embedded node data.
func (n *ExitNode) Base() *Node {
	if n == nil {
		return nil
	}
	return &n.Node
}

// ReturnNode represents a return-statement node.
type ReturnNode struct {
	Node
}

// Base returns the embedded node data.
func (n *ReturnNode) Base() *Node {
	if n == nil {
		return nil
	}
	return &n.Node
}

// AssignNode represents an assignment statement node.
type AssignNode struct {
	Node
	Var *Symbol
	Val NodeRef
}

// Base returns the embedded node data.
func (n *AssignNode) Base() *Node {
	if n == nil {
		return nil
	}
	return &n.Node
}

// ProcNode represents a stored procedure node.
type ProcNode struct {
	Node
}

// Base returns the embedded node data.
func (n *ProcNode) Base() *Node {
	if n == nil {
		return nil
	}
	return &n.Node
}
