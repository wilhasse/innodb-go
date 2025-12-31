package pars

import "testing"

func TestLexerBasic(t *testing.T) {
	input := "SELECT col FROM t WHERE id=1 AND name='a''b';"
	lex := NewLexer(input)
	expect := []TokenType{
		TokenSelect,
		TokenIdent,
		TokenFrom,
		TokenIdent,
		TokenWhere,
		TokenIdent,
		TokenEq,
		TokenInt,
		TokenAnd,
		TokenIdent,
		TokenEq,
		TokenString,
		TokenSemicolon,
		TokenEOF,
	}
	for i, exp := range expect {
		tok := lex.NextToken()
		if tok.Type != exp {
			t.Fatalf("token %d: expected %v got %v (%q)", i, exp, tok.Type, tok.Literal)
		}
	}
}

func TestLexerBoundTokens(t *testing.T) {
	lex := NewLexer(":lit $id")
	tok := lex.NextToken()
	if tok.Type != TokenBoundLiteral || tok.Literal != "lit" {
		t.Fatalf("bound literal: %v %q", tok.Type, tok.Literal)
	}
	tok = lex.NextToken()
	if tok.Type != TokenBoundID || tok.Literal != "id" {
		t.Fatalf("bound id: %v %q", tok.Type, tok.Literal)
	}
}

func TestLexerQuotedIdent(t *testing.T) {
	lex := NewLexer("\"my\"\"col\"")
	tok := lex.NextToken()
	if tok.Type != TokenIdent || tok.Literal != "my\"col" {
		t.Fatalf("quoted ident: %v %q", tok.Type, tok.Literal)
	}
}

func TestLexerComments(t *testing.T) {
	lex := NewLexer("SELECT -- comment\n 1")
	if tok := lex.NextToken(); tok.Type != TokenSelect {
		t.Fatalf("expected select, got %v", tok.Type)
	}
	if tok := lex.NextToken(); tok.Type != TokenInt || tok.Literal != "1" {
		t.Fatalf("expected int, got %v %q", tok.Type, tok.Literal)
	}
}
