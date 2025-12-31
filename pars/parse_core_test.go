package pars

import "testing"

func TestParseSQLWithInfoBindings(t *testing.T) {
	info := NewInfo()
	info.AddID("tbl", "users")
	info.AddID("col", "id")
	info.AddLiteral("val", []byte{0x2a}, LiteralInt, true)

	stmt, err := ParseSQLWithInfo(info, "SELECT $col FROM $tbl WHERE $col = :val")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := stmt.(*SelectStmt)
	if sel.Table != "users" {
		t.Fatalf("table=%s", sel.Table)
	}
	if len(sel.Columns) != 1 || sel.Columns[0] != "id" {
		t.Fatalf("columns=%v", sel.Columns)
	}
	eq := mustBinary(t, sel.Where)
	left := mustIdent(t, eq.Left)
	if left.Name != "id" {
		t.Fatalf("left ident=%s", left.Name)
	}
	right := mustLiteral(t, eq.Right)
	if right.Kind != TokenInt || right.Value != "42" {
		t.Fatalf("right literal=%+v", right)
	}
}

func TestParseSQLWithInfoStringLiteral(t *testing.T) {
	info := NewInfo()
	info.AddStrLiteral("name", "bob")

	stmt, err := ParseSQLWithInfo(info, "SELECT * FROM people WHERE name = :name")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := stmt.(*SelectStmt)
	eq := mustBinary(t, sel.Where)
	right := mustLiteral(t, eq.Right)
	if right.Kind != TokenString || right.Value != "bob" {
		t.Fatalf("right literal=%+v", right)
	}
}

func TestParseSQLUnboundID(t *testing.T) {
	if _, err := ParseSQLWithInfo(nil, "SELECT * FROM $tbl"); err == nil {
		t.Fatalf("expected error")
	}
}
