package eval

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/pars"
)

func TestEvalPredicate(t *testing.T) {
	expr := whereExpr(t, "SELECT * FROM t WHERE id=1 AND name='a'")
	row := makeTuple("1", "a")
	ok, err := EvalBool(expr, row, []string{"id", "name"})
	if err != nil {
		t.Fatalf("EvalBool: %v", err)
	}
	if !ok {
		t.Fatalf("expected predicate to match")
	}
	row2 := makeTuple("2", "a")
	ok, err = EvalBool(expr, row2, []string{"id", "name"})
	if err != nil {
		t.Fatalf("EvalBool: %v", err)
	}
	if ok {
		t.Fatalf("expected predicate mismatch")
	}
}

func TestEvalExprIdent(t *testing.T) {
	row := makeTuple("1", "b")
	val, err := EvalExpr(pars.IdentExpr{Name: "name"}, row, []string{"id", "name"})
	if err != nil {
		t.Fatalf("EvalExpr: %v", err)
	}
	if val.Kind != KindBytes || string(val.Bytes) != "b" {
		t.Fatalf("val=%v", val)
	}
	field := ValueToField(val)
	if string(field.Data) != "b" {
		t.Fatalf("field=%s", string(field.Data))
	}
}

func whereExpr(t *testing.T, sql string) pars.Expr {
	t.Helper()
	stmt, err := pars.ParseSQL(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := stmt.(*pars.SelectStmt)
	if !ok || sel.Where == nil {
		t.Fatalf("expected select with where")
	}
	return sel.Where
}

func makeTuple(values ...string) *data.Tuple {
	tuple := data.NewTuple(len(values))
	for i, value := range values {
		tuple.Fields[i] = data.Field{
			Data: []byte(value),
			Len:  uint32(len(value)),
		}
	}
	return tuple
}
