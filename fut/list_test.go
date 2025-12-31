package fut

import "testing"

func TestListAddFirstLast(t *testing.T) {
	list := &List{}
	one := &ListNode{Value: "one"}
	two := &ListNode{Value: "two"}
	zero := &ListNode{Value: "zero"}

	list.AddFirst(one)
	if list.First != one || list.Last != one || list.Len != 1 {
		t.Fatalf("expected one element list")
	}
	list.AddLast(two)
	if list.First != one || list.Last != two || list.Len != 2 {
		t.Fatalf("expected two element list")
	}
	if one.Next != two || two.Prev != one {
		t.Fatalf("expected one <-> two linkage")
	}
	list.AddFirst(zero)
	if list.First != zero || list.Len != 3 {
		t.Fatalf("expected zero to be first")
	}
	if zero.Next != one || one.Prev != zero {
		t.Fatalf("expected zero <-> one linkage")
	}
}

func TestListInsertAfterBefore(t *testing.T) {
	list := &List{}
	one := &ListNode{Value: "one"}
	three := &ListNode{Value: "three"}
	list.AddLast(one)
	list.AddLast(three)

	two := &ListNode{Value: "two"}
	list.InsertAfter(one, two)
	if one.Next != two || two.Next != three {
		t.Fatalf("expected one -> two -> three")
	}
	if three.Prev != two {
		t.Fatalf("expected three prev to be two")
	}

	zero := &ListNode{Value: "zero"}
	list.InsertBefore(one, zero)
	if list.First != zero {
		t.Fatalf("expected zero to be first")
	}
	if zero.Next != one || one.Prev != zero {
		t.Fatalf("expected zero <-> one linkage")
	}
}

func TestListRemove(t *testing.T) {
	list := &List{}
	one := &ListNode{Value: "one"}
	two := &ListNode{Value: "two"}
	three := &ListNode{Value: "three"}
	list.AddLast(one)
	list.AddLast(two)
	list.AddLast(three)

	list.Remove(two)
	if list.Len != 2 {
		t.Fatalf("expected length 2, got %d", list.Len)
	}
	if one.Next != three || three.Prev != one {
		t.Fatalf("expected one <-> three after removal")
	}
	if two.Next != nil || two.Prev != nil {
		t.Fatalf("expected removed node to be unlinked")
	}
	list.Remove(one)
	if list.First != three || list.Last != three {
		t.Fatalf("expected only three to remain")
	}
}
