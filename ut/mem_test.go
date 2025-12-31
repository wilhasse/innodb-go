package ut

import "testing"

func TestMallocFreeAndRealloc(t *testing.T) {
	MemVarInit()
	buf := Malloc(10)
	if len(buf) != 10 || MemTotalAllocated != 10 {
		t.Fatalf("alloc len=%d total=%d", len(buf), MemTotalAllocated)
	}
	buf = Realloc(buf, 20)
	if len(buf) != 20 || MemTotalAllocated != 20 {
		t.Fatalf("realloc len=%d total=%d", len(buf), MemTotalAllocated)
	}
	Free(buf)
	if MemTotalAllocated != 0 {
		t.Fatalf("total=%d", MemTotalAllocated)
	}
}

func TestMemcpyMemcmp(t *testing.T) {
	src := []byte{1, 2, 3}
	dst := make([]byte, 3)
	Memcpy(dst, src, 3)
	if Memcmp(dst, src, 3) != 0 {
		t.Fatalf("expected equal")
	}
	dst[2] = 4
	if Memcmp(dst, src, 3) <= 0 {
		t.Fatalf("expected dst > src")
	}
}

func TestStrHelpers(t *testing.T) {
	if Strlen("abc") != 3 {
		t.Fatalf("strlen mismatch")
	}
	if Strcmp("a", "b") >= 0 {
		t.Fatalf("strcmp mismatch")
	}
	dst := make([]byte, 4)
	n := Strlcpy(dst, "hello")
	if n != 5 {
		t.Fatalf("strlcpy len=%d", n)
	}
	if string(dst[:3]) != "hel" || dst[3] != 0 {
		t.Fatalf("strlcpy dst=%v", dst)
	}
}
