package row

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
)

func TestMergeTuples(t *testing.T) {
	left := []*data.Tuple{tupleKey(1), tupleKey(3)}
	right := []*data.Tuple{tupleKey(2), tupleKey(4)}
	merged, err := MergeTuples(left, right, 0)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(merged) != 4 {
		t.Fatalf("merged=%d", len(merged))
	}
	expect := []byte{1, 2, 3, 4}
	for i, tuple := range merged {
		if tuple.Fields[0].Data[0] != expect[i] {
			t.Fatalf("order[%d]=%d", i, tuple.Fields[0].Data[0])
		}
	}
}

func TestMergeTuplesDuplicate(t *testing.T) {
	left := []*data.Tuple{tupleKey(1), tupleKey(3)}
	right := []*data.Tuple{tupleKey(3)}
	if _, err := MergeTuples(left, right, 0); err != ErrDuplicateKey {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

func TestMergeStores(t *testing.T) {
	left := &Store{Rows: []*data.Tuple{tupleKey(1)}, PrimaryKey: 0}
	right := &Store{Rows: []*data.Tuple{tupleKey(2)}, PrimaryKey: 0}
	merged, err := MergeStores(left, right)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(merged.Rows) != 2 {
		t.Fatalf("rows=%d", len(merged.Rows))
	}
}

func tupleKey(value byte) *data.Tuple {
	return &data.Tuple{Fields: []data.Field{{Data: []byte{value}, Len: 1}}}
}
