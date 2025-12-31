package row

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
)

func TestSelectByKey(t *testing.T) {
	store := NewStore(0)
	t1 := tupleKey(1)
	t2 := tupleKey(2)
	_ = store.Insert(t1)
	_ = store.Insert(t2)

	key := data.Field{Data: []byte{0x02}, Len: 1}
	got := store.SelectByKey(key)
	if got != t2 {
		t.Fatalf("unexpected row")
	}
}

func TestSelectWhere(t *testing.T) {
	store := NewStore(-1)
	_ = store.Insert(tupleKey(1))
	_ = store.Insert(tupleKey(2))

	rows := store.SelectWhere(func(t *data.Tuple) bool {
		return len(t.Fields) > 0 && t.Fields[0].Data[0] == 2
	})
	if len(rows) != 1 {
		t.Fatalf("rows=%d", len(rows))
	}
}

func TestSelectAll(t *testing.T) {
	store := NewStore(-1)
	_ = store.Insert(tupleKey(1))
	_ = store.Insert(tupleKey(2))
	rows := store.SelectAll()
	if len(rows) != 2 {
		t.Fatalf("rows=%d", len(rows))
	}
}
