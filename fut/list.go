package fut

// List represents a doubly-linked list.
type List struct {
	First *ListNode
	Last  *ListNode
	Len   uint32
}

// ListNode is a node in a list.
type ListNode struct {
	Prev  *ListNode
	Next  *ListNode
	Value any
}

// AddFirst adds a node as the first element.
func (l *List) AddFirst(node *ListNode) {
	if l == nil || node == nil {
		return
	}
	if l.First == nil {
		l.First = node
		l.Last = node
		node.Prev = nil
		node.Next = nil
		l.Len++
		return
	}
	node.Prev = nil
	node.Next = l.First
	l.First.Prev = node
	l.First = node
	l.Len++
}

// AddLast adds a node as the last element.
func (l *List) AddLast(node *ListNode) {
	if l == nil || node == nil {
		return
	}
	if l.Last == nil {
		l.First = node
		l.Last = node
		node.Prev = nil
		node.Next = nil
		l.Len++
		return
	}
	node.Next = nil
	node.Prev = l.Last
	l.Last.Next = node
	l.Last = node
	l.Len++
}

// InsertAfter inserts node after existing.
func (l *List) InsertAfter(existing, node *ListNode) {
	if l == nil || existing == nil || node == nil {
		return
	}
	node.Prev = existing
	node.Next = existing.Next
	if existing.Next != nil {
		existing.Next.Prev = node
	} else {
		l.Last = node
	}
	existing.Next = node
	l.Len++
}

// InsertBefore inserts node before existing.
func (l *List) InsertBefore(existing, node *ListNode) {
	if l == nil || existing == nil || node == nil {
		return
	}
	node.Next = existing
	node.Prev = existing.Prev
	if existing.Prev != nil {
		existing.Prev.Next = node
	} else {
		l.First = node
	}
	existing.Prev = node
	l.Len++
}

// Remove removes a node from the list.
func (l *List) Remove(node *ListNode) {
	if l == nil || node == nil {
		return
	}
	if node.Prev != nil {
		node.Prev.Next = node.Next
	} else {
		l.First = node.Next
	}
	if node.Next != nil {
		node.Next.Prev = node.Prev
	} else {
		l.Last = node.Prev
	}
	node.Prev = nil
	node.Next = nil
	if l.Len > 0 {
		l.Len--
	}
}
