package ut

import "testing"

func TestVectorPushAndGet(t *testing.T) {
	vec := VectorCreate(2)
	VectorPush(vec, "a")
	VectorPush(vec, "b")
	VectorPush(vec, "c")

	if VectorLen(vec) != 3 {
		t.Fatalf("len=%d", VectorLen(vec))
	}
	if VectorGet(vec, 0) != "a" || VectorGet(vec, 2) != "c" {
		t.Fatalf("unexpected values: %v", VectorSlice(vec))
	}
	if VectorGet(vec, 5) != nil {
		t.Fatalf("expected nil out of range")
	}
}
