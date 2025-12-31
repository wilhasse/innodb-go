package row

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
)

func TestUpdateByKey(t *testing.T) {
	store := NewStore(0)
	row := &data.Tuple{
		Fields: []data.Field{
			{Data: []byte{0x01}, Len: 1},
			{Data: []byte("a"), Len: 1},
		},
	}
	_ = store.Insert(row)

	key := data.Field{Data: []byte{0x01}, Len: 1}
	_, err := store.UpdateByKey(key, map[int]data.Field{
		1: {Data: []byte("b"), Len: 1},
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if string(row.Fields[1].Data) != "b" {
		t.Fatalf("field1=%s", row.Fields[1].Data)
	}
	_, err = store.UpdateByKey(data.Field{Data: []byte{0x02}, Len: 1}, map[int]data.Field{})
	if err != ErrRowNotFound {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestUpdateWhere(t *testing.T) {
	store := NewStore(-1)
	_ = store.Insert(tupleKey(1))
	_ = store.Insert(tupleKey(2))

	updated := store.UpdateWhere(func(t *data.Tuple) bool {
		return t.Fields[0].Data[0] == 2
	}, map[int]data.Field{
		0: {Data: []byte{0x03}, Len: 1},
	})
	if updated != 1 {
		t.Fatalf("updated=%d", updated)
	}
	if store.Rows[1].Fields[0].Data[0] != 3 {
		t.Fatalf("expected updated key")
	}
}
