package pars

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseSQL parses a single SQL statement.
func ParseSQL(sql string) (Statement, error) {
	return ParseSQLWithInfo(nil, sql)
}

// ParseSQLWithInfo parses a single SQL statement with bound identifiers.
func ParseSQLWithInfo(info *Info, sql string) (Statement, error) {
	if strings.TrimSpace(sql) == "" {
		return nil, fmt.Errorf("pars: empty input")
	}
	parser := NewParserWithInfo(sql, info)
	return parser.Parse()
}

func boundLiteralExpr(lit BoundLiteral) (LiteralExpr, error) {
	switch lit.Type {
	case LiteralInt:
		value, err := decodeIntLiteral(lit.Value, lit.Unsigned)
		if err != nil {
			return LiteralExpr{}, err
		}
		return LiteralExpr{Value: value, Kind: TokenInt}, nil
	case LiteralString:
		return LiteralExpr{Value: string(lit.Value), Kind: TokenString}, nil
	default:
		return LiteralExpr{}, fmt.Errorf("pars: unsupported literal type %q", lit.Type)
	}
}

func decodeIntLiteral(value []byte, unsigned bool) (string, error) {
	if len(value) == 0 {
		return "0", nil
	}
	var u uint64
	for _, b := range value {
		u = (u << 8) | uint64(b)
	}
	if unsigned {
		return strconv.FormatUint(u, 10), nil
	}
	bits := uint(len(value) * 8)
	if bits < 64 {
		signBit := uint64(1) << (bits - 1)
		if u&signBit != 0 {
			u |= ^uint64(0) << bits
		}
	}
	return strconv.FormatInt(int64(u), 10), nil
}
