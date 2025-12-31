package pars

import "testing"

func TestParseSelectWhere(t *testing.T) {
	p := NewParser("SELECT a,b FROM t WHERE id=1 AND name='x';")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt")
	}
	if len(sel.Columns) != 2 || sel.Columns[0] != "a" || sel.Columns[1] != "b" {
		t.Fatalf("columns=%v", sel.Columns)
	}
	if sel.Table != "t" {
		t.Fatalf("table=%s", sel.Table)
	}
	and, ok := sel.Where.(BinaryExpr)
	if !ok || and.Op != TokenAnd {
		t.Fatalf("expected AND expr")
	}
	if _, ok := and.Left.(BinaryExpr); !ok {
		t.Fatalf("expected left binary expr")
	}
	if _, ok := and.Right.(BinaryExpr); !ok {
		t.Fatalf("expected right binary expr")
	}
}

func TestParseSelectStar(t *testing.T) {
	p := NewParser("SELECT * FROM tbl")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.Columns) != 1 || sel.Columns[0] != "*" {
		t.Fatalf("columns=%v", sel.Columns)
	}
	if sel.Table != "tbl" {
		t.Fatalf("table=%s", sel.Table)
	}
	if sel.Where != nil {
		t.Fatalf("unexpected where")
	}
}

func TestParseError(t *testing.T) {
	p := NewParser("UPDATE t")
	if _, err := p.Parse(); err == nil {
		t.Fatalf("expected error")
	}
}
