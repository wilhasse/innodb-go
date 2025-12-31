package ibuf

import (
	"testing"

	"github.com/wilhasse/innodb-go/dict"
	"github.com/wilhasse/innodb-go/ut"
)

func TestInsertBufferOps(t *testing.T) {
	InitAtDBStart()
	Insert(1, 10, []byte("a"))
	Insert(1, 10, []byte("b"))
	Insert(2, 5, []byte("c"))

	entries := Get(1, 10)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if string(entries[0].Data) != "a" || string(entries[1].Data) != "b" {
		t.Fatalf("unexpected data entries")
	}
	if Count() != 3 {
		t.Fatalf("expected count 3, got %d", Count())
	}
	Delete(1, 10)
	if Count() != 1 {
		t.Fatalf("expected count 1 after delete, got %d", Count())
	}
}

func TestShouldTry(t *testing.T) {
	idx := &dict.Index{Unique: false, Clustered: false}
	Use = UseInsert
	if !ShouldTry(idx, false) {
		t.Fatalf("expected ShouldTry true")
	}
	idx.Unique = true
	if ShouldTry(idx, false) {
		t.Fatalf("expected ShouldTry false for unique index")
	}
	if !ShouldTry(idx, true) {
		t.Fatalf("expected ShouldTry true when ignoring unique")
	}
	Use = UseNone
	if ShouldTry(idx, true) {
		t.Fatalf("expected ShouldTry false when disabled")
	}
}

func TestBitmapPage(t *testing.T) {
	if !BitmapPage(0, BitmapPageOffset) {
		t.Fatalf("expected bitmap page for uncompressed")
	}
	if BitmapPage(0, BitmapPageOffset+1) {
		t.Fatalf("expected non-bitmap page")
	}
	zipSize := uint32(ut.UnivPageSize / 2)
	if !BitmapPage(zipSize, BitmapPageOffset) {
		t.Fatalf("expected bitmap page for compressed")
	}
}
