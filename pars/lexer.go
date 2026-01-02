package pars

import (
	"strings"
	"unicode"
)

// TokenType identifies a lexical token.
type TokenType int

const (
	TokenIllegal TokenType = iota
	TokenEOF
	TokenIdent
	TokenInt
	TokenString
	TokenBoundLiteral
	TokenBoundID

	TokenAnd
	TokenOr
	TokenNot
	TokenNull
	TokenSQL
	TokenProcedure
	TokenSelect
	TokenFrom
	TokenWhere
	TokenInsert
	TokenInto
	TokenValues
	TokenUpdate
	TokenSet
	TokenDelete

	TokenLParen
	TokenRParen
	TokenComma
	TokenSemicolon
	TokenEq
	TokenStar
)

// Token holds a lexed token.
type Token struct {
	Type    TokenType
	Literal string
	Pos     int
}

// Lexer tokenizes SQL-like input.
type Lexer struct {
	input string
	pos   int
}

// NewLexer creates a lexer for the input string.
func NewLexer(input string) *Lexer {
	return &Lexer{input: input}
}

// NextToken returns the next token.
func (l *Lexer) NextToken() Token {
	if l == nil {
		return Token{Type: TokenEOF}
	}
	l.skipWhitespaceAndComments()
	if l.pos >= len(l.input) {
		return Token{Type: TokenEOF, Pos: l.pos}
	}

	ch := l.input[l.pos]
	switch ch {
	case '(':
		l.pos++
		return Token{Type: TokenLParen, Literal: "(", Pos: l.pos - 1}
	case ')':
		l.pos++
		return Token{Type: TokenRParen, Literal: ")", Pos: l.pos - 1}
	case ',':
		l.pos++
		return Token{Type: TokenComma, Literal: ",", Pos: l.pos - 1}
	case ';':
		l.pos++
		return Token{Type: TokenSemicolon, Literal: ";", Pos: l.pos - 1}
	case '=':
		l.pos++
		return Token{Type: TokenEq, Literal: "=", Pos: l.pos - 1}
	case '*':
		l.pos++
		return Token{Type: TokenStar, Literal: "*", Pos: l.pos - 1}
	case '\'':
		return l.readString()
	case '"':
		return l.readQuotedIdent()
	case ':':
		return l.readBoundLiteral()
	case '$':
		return l.readBoundID()
	}

	if isDigit(ch) {
		return l.readInt()
	}
	if isIdentStart(ch) {
		return l.readIdent()
	}

	l.pos++
	return Token{Type: TokenIllegal, Literal: string(ch), Pos: l.pos - 1}
}

func (l *Lexer) skipWhitespaceAndComments() {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if isSpace(ch) {
			l.pos++
			continue
		}
		if ch == '-' && l.peekNext() == '-' {
			l.pos += 2
			for l.pos < len(l.input) && l.input[l.pos] != '\n' {
				l.pos++
			}
			continue
		}
		if ch == '/' && l.peekNext() == '*' {
			l.pos += 2
			for l.pos+1 < len(l.input) {
				if l.input[l.pos] == '*' && l.input[l.pos+1] == '/' {
					l.pos += 2
					break
				}
				l.pos++
			}
			continue
		}
		break
	}
}

func (l *Lexer) readInt() Token {
	start := l.pos
	for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
		l.pos++
	}
	return Token{Type: TokenInt, Literal: l.input[start:l.pos], Pos: start}
}

func (l *Lexer) readIdent() Token {
	start := l.pos
	l.pos++
	for l.pos < len(l.input) && isIdentPart(l.input[l.pos]) {
		l.pos++
	}
	lit := l.input[start:l.pos]
	upper := strings.ToUpper(lit)
	if tok, ok := keywordTokens[upper]; ok {
		return Token{Type: tok, Literal: upper, Pos: start}
	}
	return Token{Type: TokenIdent, Literal: lit, Pos: start}
}

func (l *Lexer) readBoundLiteral() Token {
	start := l.pos
	l.pos++
	for l.pos < len(l.input) && isIdentPart(l.input[l.pos]) {
		l.pos++
	}
	if l.pos == start+1 {
		return Token{Type: TokenIllegal, Literal: ":", Pos: start}
	}
	return Token{Type: TokenBoundLiteral, Literal: l.input[start+1 : l.pos], Pos: start}
}

func (l *Lexer) readBoundID() Token {
	start := l.pos
	l.pos++
	for l.pos < len(l.input) && isIdentPart(l.input[l.pos]) {
		l.pos++
	}
	if l.pos == start+1 {
		return Token{Type: TokenIllegal, Literal: "$", Pos: start}
	}
	return Token{Type: TokenBoundID, Literal: l.input[start+1 : l.pos], Pos: start}
}

func (l *Lexer) readString() Token {
	start := l.pos
	l.pos++
	var b strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\'' {
			if l.peekNext() == '\'' {
				b.WriteByte('\'')
				l.pos += 2
				continue
			}
			l.pos++
			return Token{Type: TokenString, Literal: b.String(), Pos: start}
		}
		b.WriteByte(ch)
		l.pos++
	}
	return Token{Type: TokenIllegal, Literal: l.input[start:], Pos: start}
}

func (l *Lexer) readQuotedIdent() Token {
	start := l.pos
	l.pos++
	var b strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '"' {
			if l.peekNext() == '"' {
				b.WriteByte('"')
				l.pos += 2
				continue
			}
			l.pos++
			return Token{Type: TokenIdent, Literal: b.String(), Pos: start}
		}
		b.WriteByte(ch)
		l.pos++
	}
	return Token{Type: TokenIllegal, Literal: l.input[start:], Pos: start}
}

func (l *Lexer) peekNext() byte {
	if l.pos+1 >= len(l.input) {
		return 0
	}
	return l.input[l.pos+1]
}

func isSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentStart(ch byte) bool {
	return ch == '_' || unicode.IsLetter(rune(ch))
}

func isIdentPart(ch byte) bool {
	return ch == '_' || isDigit(ch) || unicode.IsLetter(rune(ch))
}

var keywordTokens = map[string]TokenType{
	"AND":       TokenAnd,
	"OR":        TokenOr,
	"NOT":       TokenNot,
	"NULL":      TokenNull,
	"SQL":       TokenSQL,
	"PROCEDURE": TokenProcedure,
	"SELECT":    TokenSelect,
	"FROM":      TokenFrom,
	"WHERE":     TokenWhere,
	"INSERT":    TokenInsert,
	"INTO":      TokenInto,
	"VALUES":    TokenValues,
	"UPDATE":    TokenUpdate,
	"SET":       TokenSet,
	"DELETE":    TokenDelete,
}
