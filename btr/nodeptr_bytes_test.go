package btr

import (
	"bytes"
	"testing"
)

func TestNodePtrBytesDecode(t *testing.T) {
	recBytes := NodePtrBytes(7, []byte("child"))
	child, key, ok := NodePtrBytesDecode(recBytes)
	if !ok {
		t.Fatalf("decode failed")
	}
	if child != 7 {
		t.Fatalf("child=%d", child)
	}
	if !bytes.Equal(key, []byte("child")) {
		t.Fatalf("key=%v", key)
	}
}
