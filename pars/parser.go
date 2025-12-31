package pars

import "fmt"

// Parser consumes tokens from a Lexer and builds an AST.
type Parser struct {
	lexer *Lexer
	cur   Token
	peek  Token
}

// NewParser creates a parser for the input string.
func NewParser(input string) *Parser {
	lex := NewLexer(input)
	p := &Parser{lexer: lex}
	p.nextToken()
	p.nextToken()
	return p
}

// Parse parses a single statement.
func (p *Parser) Parse() (Statement, error) {
	if p.cur.Type == TokenEOF {
		return nil, fmt.Errorf("pars: empty input")
	}
	switch p.cur.Type {
	case TokenSelect:
		return p.parseSelect()
	default:
		return nil, fmt.Errorf("pars: unexpected token %v", p.cur.Type)
	}
}

func (p *Parser) parseSelect() (Statement, error) {
	if p.cur.Type != TokenSelect {
		return nil, fmt.Errorf("pars: expected SELECT")
	}
	p.nextToken()

	cols, err := p.parseColumns()
	if err != nil {
		return nil, err
	}
	if p.cur.Type != TokenFrom {
		return nil, fmt.Errorf("pars: expected FROM")
	}
	p.nextToken()
	if p.cur.Type != TokenIdent {
		return nil, fmt.Errorf("pars: expected table name")
	}
	table := p.cur.Literal
	p.nextToken()

	var where Expr
	if p.cur.Type == TokenWhere {
		p.nextToken()
		where, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}

	return &SelectStmt{Columns: cols, Table: table, Where: where}, nil
}

func (p *Parser) parseColumns() ([]string, error) {
	if p.cur.Type == TokenStar {
		p.nextToken()
		return []string{"*"}, nil
	}
	var cols []string
	for {
		if p.cur.Type != TokenIdent {
			return nil, fmt.Errorf("pars: expected column")
		}
		cols = append(cols, p.cur.Literal)
		p.nextToken()
		if p.cur.Type != TokenComma {
			break
		}
		p.nextToken()
	}
	return cols, nil
}

func (p *Parser) parseExpr() (Expr, error) {
	return p.parseOr()
}

func (p *Parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.cur.Type == TokenOr {
		op := p.cur.Type
		p.nextToken()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = BinaryExpr{Op: op, Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseAnd() (Expr, error) {
	left, err := p.parseEquality()
	if err != nil {
		return nil, err
	}
	for p.cur.Type == TokenAnd {
		op := p.cur.Type
		p.nextToken()
		right, err := p.parseEquality()
		if err != nil {
			return nil, err
		}
		left = BinaryExpr{Op: op, Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseEquality() (Expr, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	if p.cur.Type == TokenEq {
		op := p.cur.Type
		p.nextToken()
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		left = BinaryExpr{Op: op, Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parsePrimary() (Expr, error) {
	switch p.cur.Type {
	case TokenIdent:
		expr := IdentExpr{Name: p.cur.Literal}
		p.nextToken()
		return expr, nil
	case TokenInt, TokenString, TokenNull:
		expr := LiteralExpr{Value: p.cur.Literal, Kind: p.cur.Type}
		p.nextToken()
		return expr, nil
	default:
		return nil, fmt.Errorf("pars: unexpected token %v", p.cur.Type)
	}
}

func (p *Parser) nextToken() {
	p.cur = p.peek
	p.peek = p.lexer.NextToken()
}
