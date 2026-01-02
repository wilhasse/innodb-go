package pars

// Statement is a parsed SQL statement.
type Statement interface {
	stmtNode()
}

// SelectStmt represents a simple SELECT statement.
type SelectStmt struct {
	Columns []string
	Table   string
	Where   Expr
}

func (SelectStmt) stmtNode() {}

// InsertStmt represents a basic INSERT statement.
type InsertStmt struct {
	Table   string
	Columns []string
	Values  []Expr
}

func (InsertStmt) stmtNode() {}

// Assignment represents a column assignment.
type Assignment struct {
	Column string
	Value  Expr
}

// UpdateStmt represents a basic UPDATE statement.
type UpdateStmt struct {
	Table       string
	Assignments []Assignment
	Where       Expr
}

func (UpdateStmt) stmtNode() {}

// DeleteStmt represents a basic DELETE statement.
type DeleteStmt struct {
	Table string
	Where Expr
}

func (DeleteStmt) stmtNode() {}

// Expr is a parsed expression node.
type Expr interface {
	exprNode()
}

// BinaryExpr represents a binary operation.
type BinaryExpr struct {
	Op    TokenType
	Left  Expr
	Right Expr
}

func (BinaryExpr) exprNode() {}

// IdentExpr represents an identifier reference.
type IdentExpr struct {
	Name string
}

func (IdentExpr) exprNode() {}

// LiteralExpr represents a literal value.
type LiteralExpr struct {
	Value string
	Kind  TokenType
}

func (LiteralExpr) exprNode() {}
