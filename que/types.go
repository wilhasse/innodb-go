package que

import "github.com/wilhasse/innodb-go/pars"

// NodeType identifies a query graph node type.
type NodeType int

const (
	NodeUnknown NodeType = iota
	NodeFork
	NodeThread
	NodeStatement
)

// Node is the common interface for query graph nodes.
type Node interface {
	NodeType() NodeType
	Parent() Node
	SetParent(Node)
	Next() Node
	SetNext(Node)
}

// BaseNode stores node links shared by graph nodes.
type BaseNode struct {
	nodeType NodeType
	parent   Node
	next     Node
}

// NewBaseNode initializes a base node.
func NewBaseNode(nodeType NodeType, parent Node) BaseNode {
	return BaseNode{nodeType: nodeType, parent: parent}
}

// NodeType reports the node type.
func (n *BaseNode) NodeType() NodeType {
	if n == nil {
		return NodeUnknown
	}
	return n.nodeType
}

// Parent returns the parent node.
func (n *BaseNode) Parent() Node {
	if n == nil {
		return nil
	}
	return n.parent
}

// SetParent assigns the parent node.
func (n *BaseNode) SetParent(parent Node) {
	if n == nil {
		return
	}
	n.parent = parent
}

// Next returns the next sibling node.
func (n *BaseNode) Next() Node {
	if n == nil {
		return nil
	}
	return n.next
}

// SetNext assigns the next sibling node.
func (n *BaseNode) SetNext(next Node) {
	if n == nil {
		return
	}
	n.next = next
}

// ForkType describes the query fork purpose.
type ForkType int

const (
	ForkSelectNonScroll ForkType = iota + 1
	ForkSelectScroll
	ForkInsert
	ForkUpdate
	ForkRollback
	ForkPurge
	ForkExecute
	ForkProcedure
	ForkProcedureCall
	ForkUserInterface
	ForkRecovery
)

// ForkState describes the fork execution state.
type ForkState int

const (
	ForkActive ForkState = iota + 1
	ForkCommandWait
	ForkInvalid
	ForkBeingFreed
)

// ThrState describes the thread execution state.
type ThrState int

const (
	ThrRunning ThrState = iota + 1
	ThrProcedureWait
	ThrCompleted
	ThrCommandWait
	ThrLockWait
	ThrSigReplyWait
	ThrSuspended
	ThrError
)

// LockState describes current lock wait reason.
type LockState int

const (
	LockNoLock LockState = iota
	LockRow
	LockTable
)

// Fork is a query graph fork node.
type Fork struct {
	BaseNode
	Graph    *Fork
	ForkType ForkType
	State    ForkState
	Threads  []*Thr
	SymTab   *pars.SymTab
	Info     *pars.Info
}

// Thr is a query graph thread node.
type Thr struct {
	BaseNode
	Graph     *Fork
	State     ThrState
	IsActive  bool
	Child     Node
	RunNode   Node
	PrevNode  Node
	Resource  uint64
	LockState LockState
}

// Session stores graphs published for a session.
type Session struct {
	Graphs []*Fork
}
