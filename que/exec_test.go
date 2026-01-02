package que

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/row"
)

func TestQueryGraphInsertUpdateDelete(t *testing.T) {
	store := row.NewStore(0)
	tuple1 := makeTuple("1", "a")
	tuple2 := makeTuple("1", "b")

	graph := ForkCreate(nil, nil, ForkExecute)
	thr := ThrCreate(graph)
	insert := NewInsertNode(thr, store, tuple1)
	update := NewUpdateNode(thr, store, tuple1, tuple2)
	deleteNode := NewDeleteNode(thr, store, tuple2)
	insert.SetNext(update)
	update.SetNext(deleteNode)
	thr.Child = insert

	if err := ForkRun(graph); err != nil {
		t.Fatalf("ForkRun: %v", err)
	}
	if thr.State != ThrCompleted {
		t.Fatalf("thr state=%v, want %v", thr.State, ThrCompleted)
	}
	if graph.State != ForkCommandWait {
		t.Fatalf("fork state=%v, want %v", graph.State, ForkCommandWait)
	}
	if len(store.Rows) != 0 {
		t.Fatalf("expected store to be empty after delete")
	}
	if thr.PrevNode != deleteNode {
		t.Fatalf("expected last node to be delete")
	}
}

func makeTuple(values ...string) *data.Tuple {
	tuple := data.NewTuple(len(values))
	for i, value := range values {
		tuple.Fields[i] = data.Field{
			Data: []byte(value),
			Len:  uint32(len(value)),
		}
	}
	return tuple
}
