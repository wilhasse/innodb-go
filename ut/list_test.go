package ut

import "testing"

func TestListAddRemove(t *testing.T) {
	list := ListCreate()
	if ListFirst(list) != nil || ListLast(list) != nil {
		t.Fatalf("expected empty list")
	}

	node1 := ListAddLast(list, "a")
	node2 := ListAddLast(list, "b")
	if list.First != node1 || list.Last != node2 {
		t.Fatalf("unexpected endpoints")
	}
	if node1.Next != node2 || node2.Prev != node1 {
		t.Fatalf("unexpected links")
	}

	node0 := ListAddAfter(list, nil, "start")
	if list.First != node0 || node0.Next != node1 {
		t.Fatalf("expected new head")
	}

	ListRemove(list, node1)
	if node0.Next != node2 || node2.Prev != node0 {
		t.Fatalf("remove failed")
	}
	if node1.Next != nil || node1.Prev != nil {
		t.Fatalf("node not detached")
	}

	ListFree(list)
	if list.First != nil || list.Last != nil {
		t.Fatalf("expected cleared list")
	}
}
