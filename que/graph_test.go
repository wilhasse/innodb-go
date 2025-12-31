package que

import "testing"

func TestForkAndThreadCreation(t *testing.T) {
	root := ForkCreate(nil, nil, ForkSelectNonScroll)
	if root.Graph != root {
		t.Fatalf("expected root graph")
	}
	if root.ForkType != ForkSelectNonScroll {
		t.Fatalf("fork type=%v", root.ForkType)
	}
	thr := ThrCreate(root)
	if thr == nil {
		t.Fatalf("thr is nil")
	}
	if thr.Parent() != root {
		t.Fatalf("unexpected parent")
	}
	if len(root.Threads) != 1 || root.Threads[0] != thr {
		t.Fatalf("threads=%v", root.Threads)
	}
	stmt := &BaseNode{nodeType: NodeStatement}
	thr.Child = stmt
	NodeSetParent(stmt, thr)
	if ForkGetChild(root) != stmt {
		t.Fatalf("unexpected child")
	}
}

func TestGraphPublish(t *testing.T) {
	root := ForkCreate(nil, nil, ForkSelectNonScroll)
	sess := &Session{}
	GraphPublish(root, sess)
	if len(sess.Graphs) != 1 || sess.Graphs[0] != root {
		t.Fatalf("graphs=%v", sess.Graphs)
	}
}

func TestGraphFree(t *testing.T) {
	root := ForkCreate(nil, nil, ForkSelectNonScroll)
	thr := ThrCreate(root)
	stmt := &BaseNode{nodeType: NodeStatement}
	thr.Child = stmt
	NodeSetParent(stmt, thr)

	GraphFree(root)

	if root.Graph != nil {
		t.Fatalf("expected graph cleared")
	}
	if len(root.Threads) != 0 {
		t.Fatalf("threads=%v", root.Threads)
	}
	if thr.Parent() != nil || thr.Child != nil {
		t.Fatalf("thread not cleared")
	}
	if stmt.Parent() != nil || stmt.Next() != nil {
		t.Fatalf("statement not cleared")
	}
}
