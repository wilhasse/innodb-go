package pars

import (
	"fmt"
	"strconv"
)

// SymbolType describes the kind of symbol stored in the table.
type SymbolType int

const (
	SymbolLiteral SymbolType = iota
	SymbolIdentifier
)

// Symbol represents an entry in the symbol table.
type Symbol struct {
	Type     SymbolType
	Name     string
	Literal  LiteralExpr
	Resolved bool
}

// SymTab stores symbols discovered during parsing.
type SymTab struct {
	Symbols []*Symbol
	info    *Info
}

// NewSymTab creates a new symbol table.
func NewSymTab(info *Info) *SymTab {
	return &SymTab{info: info}
}

// FreePrivate clears dynamically allocated symbol data.
func (tab *SymTab) FreePrivate() {
	if tab == nil {
		return
	}
	tab.Symbols = nil
}

// AddIntLiteral stores an integer literal.
func (tab *SymTab) AddIntLiteral(value uint64) *Symbol {
	lit := LiteralExpr{Value: strconv.FormatUint(value, 10), Kind: TokenInt}
	sym := &Symbol{Type: SymbolLiteral, Literal: lit, Resolved: true}
	tab.Symbols = append(tab.Symbols, sym)
	return sym
}

// AddStrLiteral stores a string literal.
func (tab *SymTab) AddStrLiteral(value string) *Symbol {
	lit := LiteralExpr{Value: value, Kind: TokenString}
	sym := &Symbol{Type: SymbolLiteral, Literal: lit, Resolved: true}
	tab.Symbols = append(tab.Symbols, sym)
	return sym
}

// AddNullLiteral stores a NULL literal.
func (tab *SymTab) AddNullLiteral() *Symbol {
	lit := LiteralExpr{Value: "NULL", Kind: TokenNull}
	sym := &Symbol{Type: SymbolLiteral, Literal: lit, Resolved: true}
	tab.Symbols = append(tab.Symbols, sym)
	return sym
}

// AddID stores an identifier symbol.
func (tab *SymTab) AddID(name string) *Symbol {
	sym := &Symbol{Type: SymbolIdentifier, Name: name, Resolved: false}
	tab.Symbols = append(tab.Symbols, sym)
	return sym
}

// AddBoundLiteral stores a bound literal symbol using Info.
func (tab *SymTab) AddBoundLiteral(name string) (*Symbol, error) {
	if tab == nil || tab.info == nil {
		return nil, fmt.Errorf("pars: unbound literal %s", name)
	}
	bound, ok := tab.info.Literals[name]
	if !ok {
		return nil, fmt.Errorf("pars: unbound literal %s", name)
	}
	lit, err := boundLiteralExpr(bound)
	if err != nil {
		return nil, err
	}
	sym := &Symbol{Type: SymbolLiteral, Literal: lit, Resolved: true}
	tab.Symbols = append(tab.Symbols, sym)
	return sym, nil
}

// AddBoundID stores a bound identifier symbol using Info.
func (tab *SymTab) AddBoundID(name string) (*Symbol, error) {
	if tab == nil || tab.info == nil {
		return nil, fmt.Errorf("pars: unbound id %s", name)
	}
	bound, ok := tab.info.IDs[name]
	if !ok {
		return nil, fmt.Errorf("pars: unbound id %s", name)
	}
	sym := &Symbol{Type: SymbolIdentifier, Name: bound.ID, Resolved: false}
	tab.Symbols = append(tab.Symbols, sym)
	return sym, nil
}
