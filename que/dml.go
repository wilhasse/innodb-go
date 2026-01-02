package que

import (
	"errors"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/row"
)

// ErrInvalidDMLNode reports missing DML inputs.
var ErrInvalidDMLNode = errors.New("que: invalid dml node")

// ErrDeleteNotFound reports a missing delete target.
var ErrDeleteNotFound = errors.New("que: delete target not found")

// InsertNode executes a row insertion.
type InsertNode struct {
	BaseNode
	Store *row.Store
	Tuple *data.Tuple
}

// NewInsertNode constructs an insert node.
func NewInsertNode(parent Node, store *row.Store, tuple *data.Tuple) *InsertNode {
	return &InsertNode{
		BaseNode: NewBaseNode(NodeStatement, parent),
		Store:    store,
		Tuple:    tuple,
	}
}

// Execute runs the insert node.
func (n *InsertNode) Execute(_ *Thr) error {
	if n == nil || n.Store == nil || n.Tuple == nil {
		return ErrInvalidDMLNode
	}
	return n.Store.Insert(n.Tuple)
}

// UpdateNode executes a row update.
type UpdateNode struct {
	BaseNode
	Store    *row.Store
	OldTuple *data.Tuple
	NewTuple *data.Tuple
}

// NewUpdateNode constructs an update node.
func NewUpdateNode(parent Node, store *row.Store, oldTuple, newTuple *data.Tuple) *UpdateNode {
	return &UpdateNode{
		BaseNode: NewBaseNode(NodeStatement, parent),
		Store:    store,
		OldTuple: oldTuple,
		NewTuple: newTuple,
	}
}

// Execute runs the update node.
func (n *UpdateNode) Execute(_ *Thr) error {
	if n == nil || n.Store == nil || n.OldTuple == nil || n.NewTuple == nil {
		return ErrInvalidDMLNode
	}
	return n.Store.ReplaceTuple(n.OldTuple, n.NewTuple)
}

// DeleteNode executes a row deletion.
type DeleteNode struct {
	BaseNode
	Store *row.Store
	Tuple *data.Tuple
}

// NewDeleteNode constructs a delete node.
func NewDeleteNode(parent Node, store *row.Store, tuple *data.Tuple) *DeleteNode {
	return &DeleteNode{
		BaseNode: NewBaseNode(NodeStatement, parent),
		Store:    store,
		Tuple:    tuple,
	}
}

// Execute runs the delete node.
func (n *DeleteNode) Execute(_ *Thr) error {
	if n == nil || n.Store == nil || n.Tuple == nil {
		return ErrInvalidDMLNode
	}
	if !n.Store.RemoveTuple(n.Tuple) {
		return ErrDeleteNotFound
	}
	return nil
}
