package rec

import (
	"bytes"
	"testing"
)

func TestNodePtrEncodeDecode(t *testing.T) {
	key := []byte("k1")
	recBytes := NodePtrEncode(42, key)
	child, gotKey, ok := NodePtrDecode(recBytes)
	if !ok {
		t.Fatalf("decode failed")
	}
	if child != 42 {
		t.Fatalf("child=%d", child)
	}
	if !bytes.Equal(gotKey, key) {
		t.Fatalf("key=%v", gotKey)
	}
}
