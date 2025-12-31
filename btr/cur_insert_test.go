package btr

import "testing"

func TestCurOptimisticInsertOrderAndDuplicates(t *testing.T) {
	tree := NewTree(5, nil)
	cur := NewCur(tree)

	if !cur.OptimisticInsert([]byte("b"), []byte("vb")) {
		t.Fatalf("expected insert b")
	}
	if !cur.OptimisticInsert([]byte("a"), []byte("va")) {
		t.Fatalf("expected insert a")
	}
	if !cur.OptimisticInsert([]byte("c"), []byte("vc")) {
		t.Fatalf("expected insert c")
	}

	var got []string
	it := tree.First()
	for it != nil && it.Valid() {
		got = append(got, string(it.Key()))
		if !it.Next() {
			break
		}
	}
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("order mismatch: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order mismatch: got %v want %v", got, want)
		}
	}

	if cur.OptimisticInsert([]byte("b"), []byte("vb2")) {
		t.Fatalf("expected duplicate insert to fail")
	}
	val, ok := tree.Search([]byte("b"))
	if !ok || string(val) != "vb" {
		t.Fatalf("expected duplicate to keep original value")
	}
}
