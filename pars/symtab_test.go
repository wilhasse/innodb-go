package pars

import "testing"

func TestSymTabAddLiteralsAndIDs(t *testing.T) {
	tab := NewSymTab(nil)
	intSym := tab.AddIntLiteral(5)
	if intSym.Type != SymbolLiteral || intSym.Literal.Kind != TokenInt || intSym.Literal.Value != "5" {
		t.Fatalf("int literal=%+v", intSym)
	}
	strSym := tab.AddStrLiteral("hi")
	if strSym.Type != SymbolLiteral || strSym.Literal.Kind != TokenString || strSym.Literal.Value != "hi" {
		t.Fatalf("str literal=%+v", strSym)
	}
	nullSym := tab.AddNullLiteral()
	if nullSym.Type != SymbolLiteral || nullSym.Literal.Kind != TokenNull {
		t.Fatalf("null literal=%+v", nullSym)
	}
	idSym := tab.AddID("col")
	if idSym.Type != SymbolIdentifier || idSym.Name != "col" || idSym.Resolved {
		t.Fatalf("id symbol=%+v", idSym)
	}
	if len(tab.Symbols) != 4 {
		t.Fatalf("symbols=%d", len(tab.Symbols))
	}
	tab.FreePrivate()
	if len(tab.Symbols) != 0 {
		t.Fatalf("symbols=%d", len(tab.Symbols))
	}
}

func TestSymTabBoundValues(t *testing.T) {
	info := NewInfo()
	info.AddID("tbl", "users")
	info.AddLiteral("val", []byte{0x2a}, LiteralInt, true)

	tab := NewSymTab(info)
	boundLit, err := tab.AddBoundLiteral("val")
	if err != nil {
		t.Fatalf("bound literal: %v", err)
	}
	if boundLit.Literal.Kind != TokenInt || boundLit.Literal.Value != "42" {
		t.Fatalf("bound literal=%+v", boundLit)
	}
	boundID, err := tab.AddBoundID("tbl")
	if err != nil {
		t.Fatalf("bound id: %v", err)
	}
	if boundID.Name != "users" {
		t.Fatalf("bound id=%+v", boundID)
	}
	if _, err := tab.AddBoundID("missing"); err == nil {
		t.Fatalf("expected error for missing id")
	}
	if _, err := tab.AddBoundLiteral("missing"); err == nil {
		t.Fatalf("expected error for missing literal")
	}
}
