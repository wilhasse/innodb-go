package pars

import "fmt"

// Parser consumes tokens from a Lexer and builds an AST.
type Parser struct {
	lexer *Lexer
	cur   Token
	peek  Token
	info  *Info
}

// NewParser creates a parser for the input string.
func NewParser(input string) *Parser {
	return NewParserWithInfo(input, nil)
}

// NewParserWithInfo creates a parser with bound values.
func NewParserWithInfo(input string, info *Info) *Parser {
	lex := NewLexer(input)
	p := &Parser{lexer: lex, info: info}
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
	table, err := p.parseIdent()
	if err != nil {
		return nil, err
	}

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
		name, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		cols = append(cols, name)
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
	case TokenIdent, TokenBoundID:
		name, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		expr := IdentExpr{Name: name}
		return expr, nil
	case TokenInt, TokenString, TokenNull:
		expr := LiteralExpr{Value: p.cur.Literal, Kind: p.cur.Type}
		p.nextToken()
		return expr, nil
	case TokenBoundLiteral:
		name := p.cur.Literal
		p.nextToken()
		return p.parseBoundLiteral(name)
	default:
		return nil, fmt.Errorf("pars: unexpected token %v", p.cur.Type)
	}
}

func (p *Parser) parseIdent() (string, error) {
	switch p.cur.Type {
	case TokenIdent:
		name := p.cur.Literal
		p.nextToken()
		return name, nil
	case TokenBoundID:
		name := p.cur.Literal
		p.nextToken()
		return p.resolveBoundID(name)
	default:
		return "", fmt.Errorf("pars: expected identifier")
	}
}

func (p *Parser) parseBoundLiteral(name string) (Expr, error) {
	if p.info == nil {
		return nil, fmt.Errorf("pars: unbound literal %s", name)
	}
	lit, ok := p.info.Literals[name]
	if !ok {
		return nil, fmt.Errorf("pars: unbound literal %s", name)
	}
	expr, err := boundLiteralExpr(lit)
	if err != nil {
		return nil, err
	}
	return expr, nil
}

func (p *Parser) resolveBoundID(name string) (string, error) {
	if p.info == nil {
		return "", fmt.Errorf("pars: unbound id %s", name)
	}
	bound, ok := p.info.IDs[name]
	if !ok {
		return "", fmt.Errorf("pars: unbound id %s", name)
	}
	return bound.ID, nil
}

func (p *Parser) nextToken() {
	p.cur = p.peek
	p.peek = p.lexer.NextToken()
}
