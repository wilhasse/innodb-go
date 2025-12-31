package pars

import "testing"

func TestOptimizeNormalizeEq(t *testing.T) {
	stmt, err := NewParser("SELECT * FROM t WHERE 1 = id").Parse()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := Optimize(stmt).(*SelectStmt)
	eq := mustBinary(t, sel.Where)
	if eq.Op != TokenEq {
		t.Fatalf("expected eq op")
	}
	left := mustIdent(t, eq.Left)
	if left.Name != "id" {
		t.Fatalf("left ident=%s", left.Name)
	}
	right := mustLiteral(t, eq.Right)
	if right.Value != "1" {
		t.Fatalf("right literal=%s", right.Value)
	}
}

func TestOptimizeAndWithTrue(t *testing.T) {
	stmt, err := NewParser("SELECT * FROM t WHERE id = 1 AND 1 = 1").Parse()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := Optimize(stmt).(*SelectStmt)
	eq := mustBinary(t, sel.Where)
	if eq.Op != TokenEq {
		t.Fatalf("expected eq op")
	}
	left := mustIdent(t, eq.Left)
	if left.Name != "id" {
		t.Fatalf("left ident=%s", left.Name)
	}
	right := mustLiteral(t, eq.Right)
	if right.Value != "1" {
		t.Fatalf("right literal=%s", right.Value)
	}
}

func TestOptimizeOrWithFalse(t *testing.T) {
	stmt, err := NewParser("SELECT * FROM t WHERE 0 = 1 OR id = 2").Parse()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := Optimize(stmt).(*SelectStmt)
	eq := mustBinary(t, sel.Where)
	if eq.Op != TokenEq {
		t.Fatalf("expected eq op")
	}
	left := mustIdent(t, eq.Left)
	if left.Name != "id" {
		t.Fatalf("left ident=%s", left.Name)
	}
	right := mustLiteral(t, eq.Right)
	if right.Value != "2" {
		t.Fatalf("right literal=%s", right.Value)
	}
}

func TestOptimizeFoldEquality(t *testing.T) {
	stmt, err := NewParser("SELECT * FROM t WHERE 1 = 1").Parse()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := Optimize(stmt).(*SelectStmt)
	lit := mustLiteral(t, sel.Where)
	if lit.Kind != TokenInt || lit.Value != "1" {
		t.Fatalf("literal=%+v", lit)
	}
}

func mustBinary(t *testing.T, expr Expr) BinaryExpr {
	t.Helper()
	switch e := expr.(type) {
	case BinaryExpr:
		return e
	case *BinaryExpr:
		if e == nil {
			t.Fatalf("nil binary")
		}
		return *e
	default:
		t.Fatalf("expected binary expr, got %T", expr)
	}
	return BinaryExpr{}
}

func mustIdent(t *testing.T, expr Expr) IdentExpr {
	t.Helper()
	switch e := expr.(type) {
	case IdentExpr:
		return e
	case *IdentExpr:
		if e == nil {
			t.Fatalf("nil ident")
		}
		return *e
	default:
		t.Fatalf("expected ident expr, got %T", expr)
	}
	return IdentExpr{}
}

func mustLiteral(t *testing.T, expr Expr) LiteralExpr {
	t.Helper()
	switch e := expr.(type) {
	case LiteralExpr:
		return e
	case *LiteralExpr:
		if e == nil {
			t.Fatalf("nil literal")
		}
		return *e
	default:
		t.Fatalf("expected literal expr, got %T", expr)
	}
	return LiteralExpr{}
}
