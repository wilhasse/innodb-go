package btr

import "testing"

func TestPageReorganizeRemovesDeleted(t *testing.T) {
	tree := NewTree(5, nil)
	for _, key := range []string{"a", "b", "c", "d"} {
		tree.Insert([]byte(key), []byte("v"+key))
	}

	cur := NewCur(tree)
	if !cur.Search([]byte("b"), SearchGE) || !cur.DelMarkSetClustRec() {
		t.Fatalf("expected to mark b")
	}
	if !cur.Search([]byte("c"), SearchGE) || !cur.DelMarkSetClustRec() {
		t.Fatalf("expected to mark c")
	}

	if removed := PageReorganize(tree); removed != 2 {
		t.Fatalf("expected 2 records removed, got %d", removed)
	}
	got := scanVisibleKeys(tree)
	want := []string{"a", "d"}
	if len(got) != len(want) {
		t.Fatalf("reorg scan mismatch: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("reorg scan mismatch: got %v want %v", got, want)
		}
	}
}
