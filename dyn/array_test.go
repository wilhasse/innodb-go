package dyn

import "testing"

func TestArrayPushAndDataSize(t *testing.T) {
	arr := New()
	buf := arr.Push(10)
	if buf == nil || len(buf) != 10 {
		t.Fatalf("expected buffer of size 10")
	}
	arr.PushBytes([]byte("hello"))
	if arr.DataSize() != 15 {
		t.Fatalf("expected data size 15, got %d", arr.DataSize())
	}
}

func TestArrayOpenClose(t *testing.T) {
	arr := New()
	buf := arr.Open(10)
	if buf == nil || len(buf) != 10 {
		t.Fatalf("expected open buffer of size 10")
	}
	copy(buf, []byte("abc"))
	arr.Close(3)
	if arr.DataSize() != 3 {
		t.Fatalf("expected data size 3, got %d", arr.DataSize())
	}
}

func TestArrayBlocks(t *testing.T) {
	arr := New()
	arr.Push(DynArrayDataSize)
	arr.Push(1)
	if arr.LastBlock() == arr.FirstBlock() {
		t.Fatalf("expected additional block")
	}
	if arr.NextBlock(arr.FirstBlock()) == nil {
		t.Fatalf("expected next block")
	}
}

func TestArrayGetElement(t *testing.T) {
	arr := New()
	arr.PushBytes([]byte("abcdef"))
	elem := arr.GetElement(2)
	if elem == nil || elem[0] != 'c' {
		t.Fatalf("unexpected element data")
	}
}
