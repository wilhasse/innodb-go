package que

import (
	"errors"
	"fmt"
	"strings"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/pars"
	"github.com/wilhasse/innodb-go/row"
)

// TableContext describes table metadata for graph building.
type TableContext struct {
	Store   *row.Store
	Columns []string
}

// BuildContext maps table names to stores and columns.
type BuildContext struct {
	Tables map[string]*TableContext
}

var (
	ErrMissingContext   = errors.New("que: missing build context")
	ErrMissingTable     = errors.New("que: missing table context")
	ErrInvalidStatement = errors.New("que: unsupported statement")
	ErrMissingColumns   = errors.New("que: missing columns")
	ErrMissingWhere     = errors.New("que: missing where clause")
	ErrRowNotFound      = errors.New("que: row not found")
)

// BuildGraph converts a parsed statement into a query graph.
func BuildGraph(stmt pars.Statement, ctx *BuildContext) (*Fork, error) {
	if stmt == nil {
		return nil, ErrInvalidStatement
	}
	if ctx == nil {
		return nil, ErrMissingContext
	}
	graph := ForkCreate(nil, nil, ForkExecute)
	thr := ThrCreate(graph)
	node, err := buildNode(stmt, thr, ctx)
	if err != nil {
		return nil, err
	}
	thr.Child = node
	return graph, nil
}

func buildNode(stmt pars.Statement, parent Node, ctx *BuildContext) (Node, error) {
	switch st := stmt.(type) {
	case *pars.InsertStmt:
		return buildInsertNode(st, parent, ctx)
	case *pars.UpdateStmt:
		return buildUpdateNode(st, parent, ctx)
	case *pars.DeleteStmt:
		return buildDeleteNode(st, parent, ctx)
	case *pars.SelectStmt:
		return buildSelectNode(st, parent, ctx)
	default:
		return nil, ErrInvalidStatement
	}
}

func buildInsertNode(stmt *pars.InsertStmt, parent Node, ctx *BuildContext) (Node, error) {
	table, err := tableContext(ctx, stmt.Table)
	if err != nil {
		return nil, err
	}
	if len(table.Columns) == 0 {
		return nil, ErrMissingColumns
	}
	tuple, err := buildInsertTuple(stmt, table)
	if err != nil {
		return nil, err
	}
	return NewInsertNode(parent, table.Store, tuple), nil
}

func buildUpdateNode(stmt *pars.UpdateStmt, parent Node, ctx *BuildContext) (Node, error) {
	table, err := tableContext(ctx, stmt.Table)
	if err != nil {
		return nil, err
	}
	if stmt.Where == nil {
		return nil, ErrMissingWhere
	}
	pred, err := buildPredicate(stmt.Where, table)
	if err != nil {
		return nil, err
	}
	target := findRow(table.Store, pred)
	if target == nil {
		return nil, ErrRowNotFound
	}
	next := row.CopyRow(target, row.CopyData)
	if err := applyAssignments(next, stmt.Assignments, table); err != nil {
		return nil, err
	}
	return NewUpdateNode(parent, table.Store, target, next), nil
}

func buildDeleteNode(stmt *pars.DeleteStmt, parent Node, ctx *BuildContext) (Node, error) {
	table, err := tableContext(ctx, stmt.Table)
	if err != nil {
		return nil, err
	}
	if stmt.Where == nil {
		return nil, ErrMissingWhere
	}
	pred, err := buildPredicate(stmt.Where, table)
	if err != nil {
		return nil, err
	}
	target := findRow(table.Store, pred)
	if target == nil {
		return nil, ErrRowNotFound
	}
	return NewDeleteNode(parent, table.Store, target), nil
}

func buildSelectNode(stmt *pars.SelectStmt, parent Node, ctx *BuildContext) (Node, error) {
	table, err := tableContext(ctx, stmt.Table)
	if err != nil {
		return nil, err
	}
	columns, err := mapColumns(stmt.Columns, table)
	if err != nil {
		return nil, err
	}
	var pred func(*data.Tuple) bool
	if stmt.Where != nil {
		pred, err = buildPredicate(stmt.Where, table)
		if err != nil {
			return nil, err
		}
	}
	return NewSelectNode(parent, table.Store, columns, pred), nil
}

func tableContext(ctx *BuildContext, name string) (*TableContext, error) {
	if ctx == nil || ctx.Tables == nil {
		return nil, ErrMissingContext
	}
	table := ctx.Tables[name]
	if table == nil || table.Store == nil {
		return nil, ErrMissingTable
	}
	return table, nil
}

func buildInsertTuple(stmt *pars.InsertStmt, table *TableContext) (*data.Tuple, error) {
	columns := stmt.Columns
	if len(columns) == 0 {
		columns = table.Columns
	}
	if len(columns) != len(stmt.Values) {
		return nil, fmt.Errorf("que: insert column/value mismatch")
	}
	tuple := data.NewTuple(len(table.Columns))
	for i := range tuple.Fields {
		tuple.Fields[i].Len = data.UnivSQLNull
	}
	for i, col := range columns {
		idx, ok := columnIndex(table.Columns, col)
		if !ok {
			return nil, fmt.Errorf("que: unknown column %s", col)
		}
		field, err := fieldFromExpr(stmt.Values[i])
		if err != nil {
			return nil, err
		}
		tuple.Fields[idx] = field
	}
	return tuple, nil
}

func applyAssignments(tuple *data.Tuple, assigns []pars.Assignment, table *TableContext) error {
	if tuple == nil {
		return ErrRowNotFound
	}
	for _, assign := range assigns {
		idx, ok := columnIndex(table.Columns, assign.Column)
		if !ok {
			return fmt.Errorf("que: unknown column %s", assign.Column)
		}
		field, err := fieldFromExpr(assign.Value)
		if err != nil {
			return err
		}
		if idx >= 0 && idx < len(tuple.Fields) {
			tuple.Fields[idx] = field
		}
	}
	return nil
}

func buildPredicate(expr pars.Expr, table *TableContext) (func(*data.Tuple) bool, error) {
	switch e := expr.(type) {
	case pars.BinaryExpr:
		switch e.Op {
		case pars.TokenAnd, pars.TokenOr:
			left, err := buildPredicate(e.Left, table)
			if err != nil {
				return nil, err
			}
			right, err := buildPredicate(e.Right, table)
			if err != nil {
				return nil, err
			}
			if e.Op == pars.TokenAnd {
				return func(row *data.Tuple) bool { return left(row) && right(row) }, nil
			}
			return func(row *data.Tuple) bool { return left(row) || right(row) }, nil
		case pars.TokenEq:
			return buildEqPredicate(e.Left, e.Right, table)
		default:
			return nil, fmt.Errorf("que: unsupported predicate op %v", e.Op)
		}
	default:
		return nil, fmt.Errorf("que: unsupported predicate")
	}
}

func buildEqPredicate(left, right pars.Expr, table *TableContext) (func(*data.Tuple) bool, error) {
	var ident pars.IdentExpr
	var lit pars.LiteralExpr
	switch l := left.(type) {
	case pars.IdentExpr:
		ident = l
		r, ok := right.(pars.LiteralExpr)
		if !ok {
			return nil, fmt.Errorf("que: expected literal in predicate")
		}
		lit = r
	case pars.LiteralExpr:
		r, ok := right.(pars.IdentExpr)
		if !ok {
			return nil, fmt.Errorf("que: expected identifier in predicate")
		}
		ident = r
		lit = l
	default:
		return nil, fmt.Errorf("que: unsupported predicate")
	}
	idx, ok := columnIndex(table.Columns, ident.Name)
	if !ok {
		return nil, fmt.Errorf("que: unknown column %s", ident.Name)
	}
	field, err := fieldFromExpr(lit)
	if err != nil {
		return nil, err
	}
	return func(row *data.Tuple) bool {
		if row == nil || idx < 0 || idx >= len(row.Fields) {
			return false
		}
		return data.CompareFields(&row.Fields[idx], &field) == 0
	}, nil
}

func fieldFromExpr(expr pars.Expr) (data.Field, error) {
	lit, ok := expr.(pars.LiteralExpr)
	if !ok {
		return data.Field{}, fmt.Errorf("que: expected literal")
	}
	switch lit.Kind {
	case pars.TokenNull:
		return data.Field{Len: data.UnivSQLNull}, nil
	case pars.TokenInt, pars.TokenString:
		buf := []byte(lit.Value)
		return data.Field{Data: buf, Len: uint32(len(buf))}, nil
	default:
		return data.Field{}, fmt.Errorf("que: unsupported literal")
	}
}

func mapColumns(cols []string, table *TableContext) ([]int, error) {
	if len(cols) == 0 || (len(cols) == 1 && cols[0] == "*") {
		return nil, nil
	}
	indices := make([]int, len(cols))
	for i, col := range cols {
		idx, ok := columnIndex(table.Columns, col)
		if !ok {
			return nil, fmt.Errorf("que: unknown column %s", col)
		}
		indices[i] = idx
	}
	return indices, nil
}

func columnIndex(columns []string, name string) (int, bool) {
	for i, col := range columns {
		if strings.EqualFold(col, name) {
			return i, true
		}
	}
	return -1, false
}

func findRow(store *row.Store, pred func(*data.Tuple) bool) *data.Tuple {
	if store == nil || pred == nil {
		return nil
	}
	for _, row := range store.Rows {
		if row == nil {
			continue
		}
		if pred(row) {
			return row
		}
	}
	return nil
}
