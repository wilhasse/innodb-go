package rec

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
)

func TestCompareRecordsPrefix(t *testing.T) {
	a := []data.Field{field("abcdef"), field("zzz")}
	b := []data.Field{field("abcxyz"), field("aaa")}

	if cmp := CompareRecords(a, b, []int{0}, []int{3}); cmp != 0 {
		t.Fatalf("prefix len 3 cmp=%d", cmp)
	}
	if cmp := CompareRecords(a, b, []int{0}, []int{4}); cmp >= 0 {
		t.Fatalf("prefix len 4 cmp=%d", cmp)
	}
}

func TestCompareRecordsOrder(t *testing.T) {
	a := []data.Field{field("a1"), field("b1")}
	b := []data.Field{field("a0"), field("b2")}
	cmp := CompareRecords(a, b, []int{1, 0}, nil)
	if cmp >= 0 {
		t.Fatalf("order compare cmp=%d", cmp)
	}
}

func TestCompareRecordsNull(t *testing.T) {
	a := []data.Field{nullField()}
	b := []data.Field{field("x")}
	if cmp := CompareRecords(a, b, []int{0}, nil); cmp != -1 {
		t.Fatalf("null compare cmp=%d", cmp)
	}
}

func TestCompareTuplesNil(t *testing.T) {
	if cmp := CompareTuples(nil, nil, nil, nil); cmp != 0 {
		t.Fatalf("nil compare cmp=%d", cmp)
	}
}

func field(s string) data.Field {
	return data.Field{Data: []byte(s), Len: uint32(len(s))}
}

func nullField() data.Field {
	return data.Field{Len: data.UnivSQLNull}
}
