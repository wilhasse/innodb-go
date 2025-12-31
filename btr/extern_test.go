package btr

import (
	"bytes"
	"testing"

	"github.com/wilhasse/innodb-go/fil"
)

func TestExternFieldStorage(t *testing.T) {
	fil.VarInit()

	tree := NewTree(4, nil)
	longVal := bytes.Repeat([]byte("x"), ExternFieldThreshold+5)
	stored := StoreBigRecExternFields(longVal, 8)
	tree.Insert([]byte("a"), stored)

	if got := RecGetExternallyStoredLen(stored); got != len(longVal) {
		t.Fatalf("extern len mismatch: got %d want %d", got, len(longVal))
	}
	prefix := CopyExternallyStoredFieldPrefix(stored, 8)
	if !bytes.Equal(prefix, longVal[:8]) {
		t.Fatalf("prefix mismatch: got %q want %q", prefix, longVal[:8])
	}
	full := GetExternallyStoredField(stored)
	if !bytes.Equal(full, longVal) {
		t.Fatalf("extern fetch mismatch")
	}

	RecFreeExternallyStoredFields(stored)
	if got := GetExternallyStoredField(stored); got != nil {
		t.Fatalf("expected freed extern data to be missing")
	}
}
