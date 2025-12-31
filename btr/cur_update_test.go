package btr

import "testing"

func TestCurUpdatePaths(t *testing.T) {
	tree := NewTree(5, nil)
	tree.Insert([]byte("a"), []byte("one"))
	cur := NewCur(tree)
	if !cur.Search([]byte("a"), SearchGE) {
		t.Fatalf("expected to find a")
	}

	if !cur.UpdateInPlace([]byte("uno")) {
		t.Fatalf("expected in-place update")
	}
	val, ok := tree.Search([]byte("a"))
	if !ok || string(val) != "uno" {
		t.Fatalf("expected updated value uno, got %q", val)
	}

	if cur.OptimisticUpdate([]byte("four")) {
		t.Fatalf("expected optimistic update to fail on size change")
	}
	if !cur.PessimisticUpdate([]byte("four")) {
		t.Fatalf("expected pessimistic update to succeed")
	}
	val, ok = tree.Search([]byte("a"))
	if !ok || string(val) != "four" {
		t.Fatalf("expected updated value four, got %q", val)
	}
}
