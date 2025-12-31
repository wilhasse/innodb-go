package pars

import "strconv"

// Optimize applies simple query optimizations to a parsed statement.
func Optimize(stmt Statement) Statement {
	switch s := stmt.(type) {
	case nil:
		return nil
	case *SelectStmt:
		if s == nil {
			return s
		}
		out := *s
		out.Columns = append([]string(nil), s.Columns...)
		out.Where = OptimizeExpr(s.Where)
		return &out
	case SelectStmt:
		out := s
		out.Columns = append([]string(nil), s.Columns...)
		out.Where = OptimizeExpr(s.Where)
		return &out
	default:
		return stmt
	}
}

// OptimizeExpr rewrites expressions with basic constant folding.
func OptimizeExpr(expr Expr) Expr {
	switch e := expr.(type) {
	case nil:
		return nil
	case BinaryExpr:
		return optimizeBinary(e)
	case *BinaryExpr:
		if e == nil {
			return nil
		}
		return optimizeBinary(*e)
	default:
		return expr
	}
}

func optimizeBinary(expr BinaryExpr) Expr {
	left := OptimizeExpr(expr.Left)
	right := OptimizeExpr(expr.Right)

	switch expr.Op {
	case TokenAnd:
		return optimizeAnd(left, right)
	case TokenOr:
		return optimizeOr(left, right)
	case TokenEq:
		left, right = normalizeEquality(left, right)
		if folded, ok := foldEquality(left, right); ok {
			return folded
		}
		return BinaryExpr{Op: expr.Op, Left: left, Right: right}
	default:
		return BinaryExpr{Op: expr.Op, Left: left, Right: right}
	}
}

func optimizeAnd(left, right Expr) Expr {
	if lv, ok := boolConst(left); ok {
		if !lv {
			return boolLiteral(false)
		}
		if rv, ok := boolConst(right); ok {
			return boolLiteral(lv && rv)
		}
		return right
	}
	if rv, ok := boolConst(right); ok {
		if !rv {
			return boolLiteral(false)
		}
		return left
	}
	return BinaryExpr{Op: TokenAnd, Left: left, Right: right}
}

func optimizeOr(left, right Expr) Expr {
	if lv, ok := boolConst(left); ok {
		if lv {
			return boolLiteral(true)
		}
		if rv, ok := boolConst(right); ok {
			return boolLiteral(lv || rv)
		}
		return right
	}
	if rv, ok := boolConst(right); ok {
		if rv {
			return boolLiteral(true)
		}
		return left
	}
	return BinaryExpr{Op: TokenOr, Left: left, Right: right}
}

func normalizeEquality(left, right Expr) (Expr, Expr) {
	if isLiteral(left) && isIdent(right) {
		return right, left
	}
	return left, right
}

func foldEquality(left, right Expr) (Expr, bool) {
	litLeft, okLeft := asLiteral(left)
	litRight, okRight := asLiteral(right)
	if !okLeft || !okRight {
		return nil, false
	}
	if litLeft.Kind == TokenNull || litRight.Kind == TokenNull {
		return nil, false
	}
	if litLeft.Kind != litRight.Kind {
		return nil, false
	}
	switch litLeft.Kind {
	case TokenInt, TokenString:
		return boolLiteral(litLeft.Value == litRight.Value), true
	default:
		return nil, false
	}
}

func boolConst(expr Expr) (bool, bool) {
	lit, ok := asLiteral(expr)
	if !ok || lit.Kind != TokenInt {
		return false, false
	}
	value, err := strconv.ParseInt(lit.Value, 10, 64)
	if err != nil {
		return false, false
	}
	return value != 0, true
}

func boolLiteral(value bool) Expr {
	if value {
		return LiteralExpr{Value: "1", Kind: TokenInt}
	}
	return LiteralExpr{Value: "0", Kind: TokenInt}
}

func asLiteral(expr Expr) (LiteralExpr, bool) {
	switch e := expr.(type) {
	case LiteralExpr:
		return e, true
	case *LiteralExpr:
		if e == nil {
			return LiteralExpr{}, false
		}
		return *e, true
	default:
		return LiteralExpr{}, false
	}
}

func isLiteral(expr Expr) bool {
	_, ok := asLiteral(expr)
	return ok
}

func isIdent(expr Expr) bool {
	switch expr.(type) {
	case IdentExpr, *IdentExpr:
		return true
	default:
		return false
	}
}
