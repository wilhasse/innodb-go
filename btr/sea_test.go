package btr

import (
	"bytes"
	"testing"
)

func TestAdaptiveSearchCursorCache(t *testing.T) {
	SearchVarInit()
	SearchSysCreate(8)
	SearchEnable()

	tree := NewTree(4, nil)
	tree.Insert([]byte("a"), []byte("va"))
	tree.Insert([]byte("b"), []byte("vb"))

	cur, ok := AdaptiveSearchCursor(tree, []byte("b"))
	if !ok || cur == nil || !cur.Valid() || !bytes.Equal(cur.Key(), []byte("b")) {
		t.Fatalf("expected initial search to find b")
	}
	if SearchNHashFail != 1 || SearchNSucc != 0 {
		t.Fatalf("expected one miss after first lookup")
	}

	cur, ok = AdaptiveSearchCursor(tree, []byte("b"))
	if !ok || cur == nil || !cur.Valid() || !bytes.Equal(cur.Key(), []byte("b")) {
		t.Fatalf("expected cached search to find b")
	}
	if SearchNSucc != 1 {
		t.Fatalf("expected one cache hit")
	}

	tree.Insert([]byte("c"), []byte("vc"))
	_, _ = AdaptiveSearchCursor(tree, []byte("b"))
	if SearchNHashFail != 2 {
		t.Fatalf("expected cache to invalidate after mutation")
	}
}

func TestAdaptiveSearchDisabled(t *testing.T) {
	SearchVarInit()
	SearchSysCreate(8)
	SearchDisable()

	tree := NewTree(4, nil)
	tree.Insert([]byte("a"), []byte("va"))

	cur, ok := AdaptiveSearchCursor(tree, []byte("a"))
	if !ok || cur == nil || !cur.Valid() || !bytes.Equal(cur.Key(), []byte("a")) {
		t.Fatalf("expected search to find a even when disabled")
	}
	if SearchNSucc != 0 || SearchNHashFail != 0 {
		t.Fatalf("expected counters to remain unchanged when disabled")
	}
}
