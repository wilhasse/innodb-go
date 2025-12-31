package ut

// List is a simple doubly linked list.
type List struct {
	First      *ListNode
	Last       *ListNode
	IsHeapList bool
}

// ListNode holds a list element.
type ListNode struct {
	Prev *ListNode
	Next *ListNode
	Data any
}

// ListHelper stores auxiliary data for list nodes.
type ListHelper struct {
	Data any
}

// ListCreate allocates a new list.
func ListCreate() *List {
	return &List{}
}

// ListFree clears a list.
func ListFree(list *List) {
	if list == nil {
		return
	}
	list.First = nil
	list.Last = nil
}

// ListAddLast appends data to the end of the list.
func ListAddLast(list *List, data any) *ListNode {
	if list == nil {
		return nil
	}
	return ListAddAfter(list, list.Last, data)
}

// ListAddAfter adds data after prev (or at start if prev is nil).
func ListAddAfter(list *List, prev *ListNode, data any) *ListNode {
	if list == nil {
		return nil
	}
	node := &ListNode{Data: data}
	if list.First == nil {
		node.Prev = nil
		node.Next = nil
		list.First = node
		list.Last = node
		return node
	}
	if prev == nil {
		node.Next = list.First
		list.First.Prev = node
		list.First = node
		return node
	}
	node.Prev = prev
	node.Next = prev.Next
	prev.Next = node
	if node.Next != nil {
		node.Next.Prev = node
	} else {
		list.Last = node
	}
	return node
}

// ListRemove removes a node from the list.
func ListRemove(list *List, node *ListNode) {
	if list == nil || node == nil {
		return
	}
	if node.Prev != nil {
		node.Prev.Next = node.Next
	} else {
		list.First = node.Next
	}
	if node.Next != nil {
		node.Next.Prev = node.Prev
	} else {
		list.Last = node.Prev
	}
	node.Prev = nil
	node.Next = nil
}

// ListFirst returns the first node.
func ListFirst(list *List) *ListNode {
	if list == nil {
		return nil
	}
	return list.First
}

// ListLast returns the last node.
func ListLast(list *List) *ListNode {
	if list == nil {
		return nil
	}
	return list.Last
}
