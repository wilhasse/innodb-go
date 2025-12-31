package rem

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
)

func TestCompareDataNulls(t *testing.T) {
	cmp := CompareData(data.DataVarchar, 0, []byte("a"), 1, nil, data.UnivSQLNull)
	if cmp != -1 {
		t.Fatalf("expected non-null < null, got %d", cmp)
	}
	cmp = CompareData(data.DataVarchar, 0, nil, data.UnivSQLNull, []byte("a"), 1)
	if cmp != 1 {
		t.Fatalf("expected null > non-null, got %d", cmp)
	}
	cmp = CompareData(data.DataVarchar, 0, nil, data.UnivSQLNull, nil, data.UnivSQLNull)
	if cmp != 0 {
		t.Fatalf("expected null == null, got %d", cmp)
	}
}

func TestCompareInt(t *testing.T) {
	cmp := CompareData(data.DataInt, data.DataUnsigned, []byte{0x02}, 1, []byte{0x01}, 1)
	if cmp != 1 {
		t.Fatalf("expected 2 > 1, got %d", cmp)
	}
	cmp = CompareData(data.DataInt, 0, []byte{0xff}, 1, []byte{0x00}, 1)
	if cmp != -1 {
		t.Fatalf("expected -1 < 0, got %d", cmp)
	}
}

func TestCompareCharPadding(t *testing.T) {
	cmp := CompareData(data.DataChar, 0, []byte("a"), 1, []byte("a "), 2)
	if cmp != 0 {
		t.Fatalf("expected padded equal, got %d", cmp)
	}
}

func TestCompareTuples(t *testing.T) {
	t1 := &data.Tuple{
		Fields: []data.Field{
			{Data: []byte{0x01}, Len: 1, Type: data.DataType{MType: data.DataInt, PrType: data.DataUnsigned}},
			{Data: []byte("a"), Len: 1, Type: data.DataType{MType: data.DataVarchar}},
		},
	}
	t2 := &data.Tuple{
		Fields: []data.Field{
			{Data: []byte{0x01}, Len: 1, Type: data.DataType{MType: data.DataInt, PrType: data.DataUnsigned}},
			{Data: []byte("b"), Len: 1, Type: data.DataType{MType: data.DataVarchar}},
		},
	}
	if cmp := CompareTuples(t1, t2); cmp != -1 {
		t.Fatalf("expected tuple1 < tuple2, got %d", cmp)
	}
}

func TestColsAreEqual(t *testing.T) {
	col1 := data.DataType{MType: data.DataVarchar, PrType: data.DataTypeFormPrtype(0, 1)}
	col2 := data.DataType{MType: data.DataVarchar, PrType: data.DataTypeFormPrtype(0, 2)}
	if ColsAreEqual(col1, col2, true) {
		t.Fatalf("expected charset mismatch")
	}
	if !ColsAreEqual(col1, col2, false) {
		t.Fatalf("expected equal without charset check")
	}
	int1 := data.DataType{MType: data.DataInt, PrType: data.DataUnsigned, Len: 4}
	int2 := data.DataType{MType: data.DataInt, PrType: 0, Len: 4}
	if ColsAreEqual(int1, int2, false) {
		t.Fatalf("expected unsigned mismatch")
	}
}
