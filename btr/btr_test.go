package btr

import (
	"bytes"
	"fmt"
	"testing"
)

func TestTreeInsertSearch(t *testing.T) {
	tree := NewTree(4, nil)

	if replaced := tree.Insert([]byte("b"), []byte("2")); replaced {
		t.Fatalf("unexpected replace on first insert")
	}
	tree.Insert([]byte("a"), []byte("1"))
	tree.Insert([]byte("c"), []byte("3"))

	val, ok := tree.Search([]byte("b"))
	if !ok || !bytes.Equal(val, []byte("2")) {
		t.Fatalf("expected value for key b, got %q ok=%v", val, ok)
	}

	if _, ok := tree.Search([]byte("d")); ok {
		t.Fatalf("unexpected value for missing key")
	}

	if replaced := tree.Insert([]byte("b"), []byte("2b")); !replaced {
		t.Fatalf("expected replace on duplicate insert")
	}
	val, ok = tree.Search([]byte("b"))
	if !ok || !bytes.Equal(val, []byte("2b")) {
		t.Fatalf("expected updated value for key b, got %q ok=%v", val, ok)
	}
}

func TestTreeSplitAndSize(t *testing.T) {
	tree := NewTree(4, nil)
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("%02d", i))
		value := []byte(fmt.Sprintf("v%02d", i))
		tree.Insert(key, value)
	}
	if tree.Size() != 20 {
		t.Fatalf("expected size 20, got %d", tree.Size())
	}

	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("%02d", i))
		value, ok := tree.Search(key)
		if !ok {
			t.Fatalf("missing key %s", key)
		}
		if !bytes.Equal(value, []byte(fmt.Sprintf("v%02d", i))) {
			t.Fatalf("unexpected value for %s", key)
		}
	}
}

func TestTreeDelete(t *testing.T) {
	tree := NewTree(4, nil)
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("%02d", i))
		tree.Insert(key, []byte(fmt.Sprintf("v%02d", i)))
	}

	for i := 0; i < 20; i += 2 {
		key := []byte(fmt.Sprintf("%02d", i))
		if !tree.Delete(key) {
			t.Fatalf("expected delete to remove %s", key)
		}
	}

	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("%02d", i))
		_, ok := tree.Search(key)
		if i%2 == 0 {
			if ok {
				t.Fatalf("expected %s to be deleted", key)
			}
		} else if !ok {
			t.Fatalf("expected %s to remain", key)
		}
	}

	if tree.Size() != 10 {
		t.Fatalf("expected size 10, got %d", tree.Size())
	}
}

func TestCursorIteration(t *testing.T) {
	tree := NewTree(4, nil)
	for _, key := range []string{"c", "a", "e", "b", "d"} {
		tree.Insert([]byte(key), []byte("v"+key))
	}

	var got []string
	cur := tree.First()
	for cur != nil && cur.Valid() {
		got = append(got, string(cur.Key()))
		if !cur.Next() {
			break
		}
	}
	expected := []string{"a", "b", "c", "d", "e"}
	if len(got) != len(expected) {
		t.Fatalf("unexpected iteration length: %v", got)
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Fatalf("unexpected iteration order: %v", got)
		}
	}

	cur = tree.Seek([]byte("c"))
	if cur == nil || !cur.Valid() || string(cur.Key()) != "c" {
		t.Fatalf("expected seek to land on c")
	}
	cur = tree.Seek([]byte("bb"))
	if cur == nil || !cur.Valid() || string(cur.Key()) != "c" {
		t.Fatalf("expected seek to land on c for bb")
	}

	cur = tree.Last()
	if cur == nil || !cur.Valid() || string(cur.Key()) != "e" {
		t.Fatalf("expected last to be e")
	}
	if !cur.Prev() || string(cur.Key()) != "d" {
		t.Fatalf("expected prev to be d")
	}
}
