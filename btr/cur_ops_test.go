package btr

import (
	"bytes"
	"testing"
)

func TestCurSearchModes(t *testing.T) {
	tree := NewTree(4, nil)
	for _, key := range []string{"b", "d", "f"} {
		tree.Insert([]byte(key), []byte("v"+key))
	}

	cur := NewCur(tree)
	if !cur.Search([]byte("c"), SearchGE) {
		t.Fatalf("expected SearchGE to find record")
	}
	if got := cur.Key(); !bytes.Equal(got, []byte("d")) {
		t.Fatalf("expected SearchGE to land on d, got %q", got)
	}

	if !cur.Search([]byte("c"), SearchLE) {
		t.Fatalf("expected SearchLE to find record")
	}
	if got := cur.Key(); !bytes.Equal(got, []byte("b")) {
		t.Fatalf("expected SearchLE to land on b, got %q", got)
	}

	if cur.Search([]byte("a"), SearchLE) {
		t.Fatalf("expected SearchLE on a to fail")
	}
}

func TestCurOpenAtIndexSide(t *testing.T) {
	tree := NewTree(4, nil)
	for _, key := range []string{"b", "a", "c"} {
		tree.Insert([]byte(key), []byte("v"+key))
	}

	cur := NewCur(tree)
	if !cur.OpenAtIndexSide(true) || string(cur.Key()) != "a" {
		t.Fatalf("expected left side to be a")
	}
	if !cur.OpenAtIndexSide(false) || string(cur.Key()) != "c" {
		t.Fatalf("expected right side to be c")
	}
}

func TestCurUpdateAndDelete(t *testing.T) {
	tree := NewTree(4, nil)
	tree.Insert([]byte("a"), []byte("va"))
	tree.Insert([]byte("b"), []byte("vb"))
	tree.Insert([]byte("c"), []byte("vc"))

	cur := NewCur(tree)
	if !cur.Search([]byte("b"), SearchGE) {
		t.Fatalf("expected to find b")
	}
	if !cur.Update([]byte("vb2")) {
		t.Fatalf("expected update to succeed")
	}
	val, ok := tree.Search([]byte("b"))
	if !ok || !bytes.Equal(val, []byte("vb2")) {
		t.Fatalf("expected updated value for b")
	}

	if !cur.Delete() {
		t.Fatalf("expected delete to succeed")
	}
	if _, ok := tree.Search([]byte("b")); ok {
		t.Fatalf("expected b to be deleted")
	}
}

func TestCurVarInit(t *testing.T) {
	CurNNonSea = 10
	CurNSea = 5
	CurNNonSeaOld = 7
	CurNSeaOld = 3

	CurVarInit()

	if CurNNonSea != 0 || CurNSea != 0 || CurNNonSeaOld != 0 || CurNSeaOld != 0 {
		t.Fatalf("expected counters to reset")
	}
}
