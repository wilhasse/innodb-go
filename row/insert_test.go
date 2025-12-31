package row

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
)

func TestStoreInsert(t *testing.T) {
	store := NewStore(0)
	t1 := &data.Tuple{Fields: []data.Field{{Data: []byte{0x01}, Len: 1}}}
	if err := store.Insert(t1); err != nil {
		t.Fatalf("insert t1: %v", err)
	}
	t2 := &data.Tuple{Fields: []data.Field{{Data: []byte{0x02}, Len: 1}}}
	if err := store.Insert(t2); err != nil {
		t.Fatalf("insert t2: %v", err)
	}
	dup := &data.Tuple{Fields: []data.Field{{Data: []byte{0x01}, Len: 1}}}
	if err := store.Insert(dup); err != ErrDuplicateKey {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

func TestStoreInsertNoPrimaryKey(t *testing.T) {
	store := NewStore(-1)
	t1 := &data.Tuple{Fields: []data.Field{{Data: []byte{0x01}, Len: 1}}}
	t2 := &data.Tuple{Fields: []data.Field{{Data: []byte{0x01}, Len: 1}}}
	if err := store.Insert(t1); err != nil {
		t.Fatalf("insert t1: %v", err)
	}
	if err := store.Insert(t2); err != nil {
		t.Fatalf("insert t2: %v", err)
	}
	if len(store.Rows) != 2 {
		t.Fatalf("rows=%d", len(store.Rows))
	}
}
