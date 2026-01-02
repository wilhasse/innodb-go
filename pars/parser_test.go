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

func TestParseInsert(t *testing.T) {
	p := NewParser("INSERT INTO t (a,b) VALUES (1,'x')")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ins, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("expected InsertStmt")
	}
	if ins.Table != "t" {
		t.Fatalf("table=%s", ins.Table)
	}
	if len(ins.Columns) != 2 || ins.Columns[0] != "a" || ins.Columns[1] != "b" {
		t.Fatalf("columns=%v", ins.Columns)
	}
	if len(ins.Values) != 2 {
		t.Fatalf("values=%d", len(ins.Values))
	}
}

func TestParseUpdate(t *testing.T) {
	p := NewParser("UPDATE t SET a=1,b='y' WHERE id=2")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	upd, ok := stmt.(*UpdateStmt)
	if !ok {
		t.Fatalf("expected UpdateStmt")
	}
	if upd.Table != "t" {
		t.Fatalf("table=%s", upd.Table)
	}
	if len(upd.Assignments) != 2 {
		t.Fatalf("assignments=%d", len(upd.Assignments))
	}
	if upd.Where == nil {
		t.Fatalf("expected where")
	}
}

func TestParseDelete(t *testing.T) {
	p := NewParser("DELETE FROM t WHERE id=1")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	del, ok := stmt.(*DeleteStmt)
	if !ok {
		t.Fatalf("expected DeleteStmt")
	}
	if del.Table != "t" {
		t.Fatalf("table=%s", del.Table)
	}
	if del.Where == nil {
		t.Fatalf("expected where")
	}
}
