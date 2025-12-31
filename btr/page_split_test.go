package btr

import "testing"

func TestPageSplitAndRootRaise(t *testing.T) {
	tree := NewTree(3, nil)
	if PageSplitAndInsert(tree, []byte("a"), []byte("va")) {
		t.Fatalf("did not expect split on first insert")
	}
	if PageSplitAndInsert(tree, []byte("b"), []byte("vb")) {
		t.Fatalf("did not expect split on second insert")
	}
	if !PageSplitAndInsert(tree, []byte("c"), []byte("vc")) {
		t.Fatalf("expected split on third insert")
	}
	if treeHeight(tree) != 1 {
		t.Fatalf("expected tree height 1 after split, got %d", treeHeight(tree))
	}

	tree2 := NewTree(3, nil)
	if RootRaiseAndInsert(tree2, []byte("a"), []byte("va")) {
		t.Fatalf("did not expect root raise on first insert")
	}
	if RootRaiseAndInsert(tree2, []byte("b"), []byte("vb")) {
		t.Fatalf("did not expect root raise on second insert")
	}
	if !RootRaiseAndInsert(tree2, []byte("c"), []byte("vc")) {
		t.Fatalf("expected root raise on third insert")
	}
	if treeHeight(tree2) != 1 {
		t.Fatalf("expected tree2 height 1 after root raise, got %d", treeHeight(tree2))
	}
}
