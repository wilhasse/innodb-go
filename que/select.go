package que

import (
	"errors"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/row"
)

// ErrInvalidSelectNode reports missing select inputs.
var ErrInvalidSelectNode = errors.New("que: invalid select node")

// SelectNode executes a simple row scan.
type SelectNode struct {
	BaseNode
	Store     *row.Store
	Columns   []int
	Predicate func(*data.Tuple) bool
	Rows      []*data.Tuple
}

// NewSelectNode constructs a select node.
func NewSelectNode(parent Node, store *row.Store, columns []int, pred func(*data.Tuple) bool) *SelectNode {
	return &SelectNode{
		BaseNode:  NewBaseNode(NodeStatement, parent),
		Store:     store,
		Columns:   columns,
		Predicate: pred,
	}
}

// Execute runs the select node and stores results.
func (n *SelectNode) Execute(_ *Thr) error {
	if n == nil || n.Store == nil {
		return ErrInvalidSelectNode
	}
	n.Rows = nil
	for _, row := range n.Store.Rows {
		if row == nil {
			continue
		}
		if n.Predicate != nil && !n.Predicate(row) {
			continue
		}
		n.Rows = append(n.Rows, projectRow(row, n.Columns))
	}
	return nil
}

func projectRow(row *data.Tuple, columns []int) *data.Tuple {
	if row == nil {
		return nil
	}
	if len(columns) == 0 {
		return row
	}
	out := data.NewTuple(len(columns))
	for i, idx := range columns {
		if idx < 0 || idx >= len(row.Fields) {
			out.Fields[i].Len = data.UnivSQLNull
			continue
		}
		out.Fields[i] = row.Fields[idx]
	}
	return out
}
