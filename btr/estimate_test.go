package btr

import "testing"

func TestEstimateHelpers(t *testing.T) {
	tree := NewTree(4, nil)
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		tree.Insert([]byte(key), []byte("v"+key))
	}
	cur := NewCur(tree)
	if !cur.Search([]byte("c"), SearchGE) || !cur.DelMarkSetClustRec() {
		t.Fatalf("expected to mark c")
	}

	if got := EstimateNRowsInRange(tree, []byte("b"), []byte("d")); got != 2 {
		t.Fatalf("range estimate mismatch: got %d want 2", got)
	}
	if got := EstimateNumberOfDifferentKeyVals(tree); got != 4 {
		t.Fatalf("distinct estimate mismatch: got %d want 4", got)
	}
}
