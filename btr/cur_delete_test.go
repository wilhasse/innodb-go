package btr

import "testing"

func TestCurDeleteMarkAndDelete(t *testing.T) {
	tree := NewTree(5, nil)
	for _, key := range []string{"a", "b", "c", "d"} {
		tree.Insert([]byte(key), []byte("v"+key))
	}

	cur := NewCur(tree)
	if !cur.Search([]byte("b"), SearchGE) {
		t.Fatalf("expected to find b")
	}
	if !cur.DelMarkSetClustRec() {
		t.Fatalf("expected delete mark for b")
	}

	got := scanVisibleKeys(tree)
	want := []string{"a", "c", "d"}
	if len(got) != len(want) {
		t.Fatalf("marked scan mismatch: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("marked scan mismatch: got %v want %v", got, want)
		}
	}

	if !cur.DelUnmarkForIbuf() {
		t.Fatalf("expected unmark for b")
	}
	got = scanVisibleKeys(tree)
	want = []string{"a", "b", "c", "d"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unmark scan mismatch: got %v want %v", got, want)
		}
	}

	if !cur.Search([]byte("c"), SearchGE) {
		t.Fatalf("expected to find c")
	}
	if !cur.OptimisticDelete() {
		t.Fatalf("expected delete for c")
	}
	got = scanVisibleKeys(tree)
	want = []string{"a", "b", "d"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delete scan mismatch: got %v want %v", got, want)
		}
	}
}

func scanVisibleKeys(tree *Tree) []string {
	cur := NewCur(tree)
	if !cur.OpenAtIndexSide(true) {
		return nil
	}
	var keys []string
	for {
		keys = append(keys, string(cur.Key()))
		if !cur.Next() {
			break
		}
	}
	return keys
}
