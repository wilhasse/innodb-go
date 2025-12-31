package btr

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestBtrTraceHash(t *testing.T) {
	trace := TraceOperations()
	sum := sha256.Sum256([]byte(trace))
	got := hex.EncodeToString(sum[:])
	const want = "8a3a3fca9dc16f32bdd31255c722103fbefb973f3e95f2e57c43458f73340277"
	if got != want {
		t.Fatalf("trace hash mismatch: got %s want %s", got, want)
	}
}
