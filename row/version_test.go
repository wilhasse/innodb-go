package row

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
)

func TestVersionedRow(t *testing.T) {
	v0 := &data.Tuple{Fields: []data.Field{{Data: []byte("a"), Len: 1}}}
	v1 := &data.Tuple{Fields: []data.Field{{Data: []byte("b"), Len: 1}}}
	v2 := &data.Tuple{Fields: []data.Field{{Data: []byte("c"), Len: 1}}}

	vr := NewVersionedRow(1, v0)
	vr.AddVersion(3, v1)
	vr.AddVersion(5, v2)

	if got := vr.VersionFor(0); got != nil {
		t.Fatalf("expected nil version")
	}
	if got := vr.VersionFor(2); got != v0 {
		t.Fatalf("expected v0")
	}
	if got := vr.VersionFor(4); got != v1 {
		t.Fatalf("expected v1")
	}
	if got := vr.VersionFor(5); got != v2 {
		t.Fatalf("expected v2")
	}
	if got := vr.Current(); got != v2 {
		t.Fatalf("expected current v2")
	}
}
