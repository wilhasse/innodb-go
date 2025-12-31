package btr

import "testing"

func TestCurSearchToNthLevelAndRandom(t *testing.T) {
	tree := NewTree(3, nil)
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		tree.Insert([]byte(key), []byte("v"+key))
	}

	cur := NewCur(tree)
	if !cur.SearchToNthLevel([]byte("c"), SearchGE, 0) {
		t.Fatalf("expected leaf search to succeed")
	}
	if string(cur.Key()) != "c" {
		t.Fatalf("expected leaf search to land on c, got %q", cur.Key())
	}
	if len(cur.Path) == 0 {
		t.Fatalf("expected path info to be recorded")
	}

	if !cur.SearchToNthLevel([]byte("c"), SearchGE, 1) {
		t.Fatalf("expected level 1 search to succeed")
	}
	if !cur.Valid() {
		t.Fatalf("expected cursor to be valid at level 1")
	}

	if !cur.OpenAtRandom() {
		t.Fatalf("expected random open to succeed")
	}
	if string(cur.Key()) != "e" {
		t.Fatalf("expected random open to land on rightmost key e, got %q", cur.Key())
	}
}
