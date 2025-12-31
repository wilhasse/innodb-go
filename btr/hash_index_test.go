package btr

import "testing"

func TestHashIndexLookupMatchesTree(t *testing.T) {
	tree := NewTree(4, nil)
	SearchSysCreate(128)
	for _, key := range []string{"a", "b", "c"} {
		tree.Insert([]byte(key), []byte("v"+key))
	}
	SearchBuildPageHashIndex(tree)

	for _, key := range []string{"a", "b", "c"} {
		valTree, okTree := tree.Search([]byte(key))
		valHash, okHash := SearchGuessOnHash(tree, []byte(key))
		if okTree != okHash || string(valTree) != string(valHash) {
			t.Fatalf("hash mismatch for %q", key)
		}
	}

	tree.Insert([]byte("d"), []byte("vd"))
	if _, ok := SearchGuessOnHash(tree, []byte("d")); !ok {
		t.Fatalf("expected hash to update on insert")
	}

	cur := NewCur(tree)
	if !cur.Search([]byte("b"), SearchGE) {
		t.Fatalf("expected to find b")
	}
	if !cur.OptimisticDelete() {
		t.Fatalf("expected delete b")
	}
	if _, ok := SearchGuessOnHash(tree, []byte("b")); ok {
		t.Fatalf("expected hash miss after delete")
	}
}
