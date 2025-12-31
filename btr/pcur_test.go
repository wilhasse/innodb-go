package btr

import (
	"bytes"
	"testing"
)

func TestPcurStoreRestoreOn(t *testing.T) {
	tree := NewTree(4, nil)
	for _, key := range []string{"a", "b", "c"} {
		tree.Insert([]byte(key), []byte("v"+key))
	}

	pcur := NewPcur(tree)
	if !pcur.OpenOnUserRec([]byte("b"), SearchGE) {
		t.Fatalf("expected to open on b")
	}
	pcur.StorePosition()

	tree.Delete([]byte("b"))

	exact := pcur.RestorePosition()
	if exact {
		t.Fatalf("expected no exact match after delete")
	}
	if !pcur.Cur.Valid() || !bytes.Equal(pcur.Cur.Key(), []byte("a")) {
		t.Fatalf("expected restore to land on a")
	}
}

func TestPcurStoreRestoreAfterLast(t *testing.T) {
	tree := NewTree(4, nil)
	tree.Insert([]byte("a"), []byte("va"))

	pcur := NewPcur(tree)
	if pcur.OpenOnUserRec([]byte("z"), SearchGE) {
		t.Fatalf("expected search beyond last to fail")
	}
	if pcur.RelPos != PcurAfterLastInTree {
		t.Fatalf("expected after last rel pos")
	}
	pcur.StorePosition()

	tree.Insert([]byte("b"), []byte("vb"))

	exact := pcur.RestorePosition()
	if exact {
		t.Fatalf("expected no exact match")
	}
	if pcur.Cur.Valid() {
		t.Fatalf("expected cursor to remain invalid")
	}
	if pcur.RelPos != PcurAfterLastInTree {
		t.Fatalf("expected rel pos to remain after last")
	}
}

func TestPcurCopyStoredPosition(t *testing.T) {
	tree := NewTree(4, nil)
	tree.Insert([]byte("a"), []byte("va"))
	tree.Insert([]byte("b"), []byte("vb"))

	pcur1 := NewPcur(tree)
	pcur1.OpenOnUserRec([]byte("b"), SearchGE)
	pcur1.StorePosition()

	pcur2 := NewPcur(tree)
	pcur2.CopyStoredPosition(pcur1)

	if !bytes.Equal(pcur2.StoredKey, pcur1.StoredKey) {
		t.Fatalf("expected stored key to copy")
	}
	if pcur2.RelPos != pcur1.RelPos || pcur2.OldStored != pcur1.OldStored {
		t.Fatalf("expected stored state to copy")
	}
}
