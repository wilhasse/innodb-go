package ha

import "testing"

func TestStoragePutMemLimit(t *testing.T) {
	store := CreateStorage(0, 0)
	data := []byte("alpha")
	first := store.PutMemLimit(data, 0)
	if first == nil {
		t.Fatalf("expected data to be stored")
	}
	second := store.PutMemLimit(data, 0)
	if &first[0] != &second[0] {
		t.Fatalf("expected duplicate to return same data")
	}
	if got := store.Size(); got != uint64(len(data)) {
		t.Fatalf("expected size %d, got %d", len(data), got)
	}
	if store.PutMemLimit([]byte("beta"), uint64(len(data))) != nil {
		t.Fatalf("expected mem limit to prevent insert")
	}
}

func TestStoragePutString(t *testing.T) {
	store := CreateStorage(0, 0)
	ptr := store.PutString("hello")
	if string(ptr) != "hello" {
		t.Fatalf("expected stored string to match")
	}
}

func TestStorageEmpty(t *testing.T) {
	store := CreateStorage(0, 0)
	store.Put([]byte("one"))
	store.Put([]byte("two"))
	store.Empty()
	if store.Size() != 0 {
		t.Fatalf("expected size 0 after empty")
	}
	if store.Put([]byte("three")) == nil {
		t.Fatalf("expected store to accept new data")
	}
}
