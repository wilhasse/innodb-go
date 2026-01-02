package eval

import (
	"fmt"
	"strings"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/pars"
)

// EvalExpr evaluates a parsed expression against a tuple row.
func EvalExpr(expr pars.Expr, row *data.Tuple, columns []string) (Value, error) {
	switch e := expr.(type) {
	case pars.LiteralExpr:
		return valueFromLiteral(e), nil
	case pars.IdentExpr:
		return valueFromIdent(e, row, columns)
	case pars.BinaryExpr:
		switch e.Op {
		case pars.TokenAnd, pars.TokenOr:
			left, err := EvalBool(e.Left, row, columns)
			if err != nil {
				return Value{}, err
			}
			right, err := EvalBool(e.Right, row, columns)
			if err != nil {
				return Value{}, err
			}
			val, err := EvalLogical(opString(e.Op), Value{Kind: KindBool, Bool: left}, Value{Kind: KindBool, Bool: right})
			if err != nil {
				return Value{}, err
			}
			return Value{Kind: KindBool, Bool: val}, nil
		case pars.TokenEq:
			left, err := EvalExpr(e.Left, row, columns)
			if err != nil {
				return Value{}, err
			}
			right, err := EvalExpr(e.Right, row, columns)
			if err != nil {
				return Value{}, err
			}
			ok, err := EvalCmp("=", left, right)
			if err != nil {
				return Value{}, err
			}
			return Value{Kind: KindBool, Bool: ok}, nil
		default:
			return Value{}, fmt.Errorf("eval: unsupported op %v", e.Op)
		}
	default:
		return Value{}, fmt.Errorf("eval: unsupported expr")
	}
}

// EvalBool evaluates an expression and returns a boolean result.
func EvalBool(expr pars.Expr, row *data.Tuple, columns []string) (bool, error) {
	val, err := EvalExpr(expr, row, columns)
	if err != nil {
		return false, err
	}
	switch val.Kind {
	case KindBool:
		return val.Bool, nil
	case KindInt:
		return val.Int != 0, nil
	case KindBytes:
		return len(val.Bytes) > 0, nil
	case KindNull:
		return false, nil
	default:
		return false, fmt.Errorf("eval: unsupported value kind")
	}
}

// ValueToField converts an evaluated value to a data field.
func ValueToField(val Value) data.Field {
	switch val.Kind {
	case KindNull:
		return data.Field{Len: data.UnivSQLNull}
	case KindBool:
		if val.Bool {
			return data.Field{Data: []byte("1"), Len: 1}
		}
		return data.Field{Data: []byte("0"), Len: 1}
	case KindInt:
		text := []byte(fmt.Sprintf("%d", val.Int))
		return data.Field{Data: text, Len: uint32(len(text))}
	case KindBytes:
		buf := append([]byte(nil), val.Bytes...)
		return data.Field{Data: buf, Len: uint32(len(buf))}
	default:
		return data.Field{Len: data.UnivSQLNull}
	}
}

func valueFromLiteral(lit pars.LiteralExpr) Value {
	switch lit.Kind {
	case pars.TokenNull:
		return Value{Kind: KindNull}
	case pars.TokenInt, pars.TokenString:
		return Value{Kind: KindBytes, Bytes: []byte(lit.Value)}
	default:
		return Value{Kind: KindNull}
	}
}

func valueFromIdent(ident pars.IdentExpr, row *data.Tuple, columns []string) (Value, error) {
	if row == nil {
		return Value{}, fmt.Errorf("eval: nil row")
	}
	idx := -1
	for i, col := range columns {
		if strings.EqualFold(col, ident.Name) {
			idx = i
			break
		}
	}
	if idx < 0 || idx >= len(row.Fields) {
		return Value{}, fmt.Errorf("eval: unknown column %s", ident.Name)
	}
	field := row.Fields[idx]
	if data.FieldIsNull(&field) {
		return Value{Kind: KindNull}, nil
	}
	limit := int(field.Len)
	if limit <= 0 || limit > len(field.Data) {
		limit = len(field.Data)
	}
	return Value{Kind: KindBytes, Bytes: append([]byte(nil), field.Data[:limit]...)}, nil
}

func opString(op pars.TokenType) string {
	switch op {
	case pars.TokenAnd:
		return "AND"
	case pars.TokenOr:
		return "OR"
	default:
		return ""
	}
}
