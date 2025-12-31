package que

// TraceOn enables query graph tracing in debug builds.
var TraceOn bool

// VarInit resets package-level state.
func VarInit() {
	TraceOn = false
}

// GraphPublish adds a graph to a session.
func GraphPublish(graph *Fork, sess *Session) {
	if graph == nil || sess == nil {
		return
	}
	sess.Graphs = append(sess.Graphs, graph)
}

// ForkCreate creates a query graph fork node.
func ForkCreate(graph *Fork, parent Node, forkType ForkType) *Fork {
	fork := &Fork{
		BaseNode: NewBaseNode(NodeFork, parent),
		ForkType: forkType,
		State:    ForkCommandWait,
	}
	if graph != nil {
		fork.Graph = graph
	} else {
		fork.Graph = fork
	}
	return fork
}

// ForkGetFirstThr returns the first thread in the fork.
func ForkGetFirstThr(fork *Fork) *Thr {
	if fork == nil || len(fork.Threads) == 0 {
		return nil
	}
	return fork.Threads[0]
}

// ForkGetChild returns the first thread child node.
func ForkGetChild(fork *Fork) Node {
	thr := ForkGetFirstThr(fork)
	if thr == nil {
		return nil
	}
	return thr.Child
}

// NodeSetParent sets the parent for a node.
func NodeSetParent(node Node, parent Node) {
	if node == nil {
		return
	}
	node.SetParent(parent)
}

// ThrCreate creates a query graph thread node.
func ThrCreate(parent *Fork) *Thr {
	if parent == nil {
		return nil
	}
	thr := &Thr{
		BaseNode: NewBaseNode(NodeThread, parent),
		Graph:    parent.Graph,
		State:    ThrCommandWait,
	}
	parent.Threads = append(parent.Threads, thr)
	return thr
}

// GraphFreeRecursive clears references in a graph subtree.
func GraphFreeRecursive(node Node) {
	if node == nil {
		return
	}
	switch n := node.(type) {
	case *Fork:
		for _, thr := range n.Threads {
			GraphFreeRecursive(thr)
		}
		n.Threads = nil
	case *Thr:
		if n.Child != nil {
			GraphFreeRecursive(n.Child)
			n.Child = nil
		}
		n.RunNode = nil
		n.PrevNode = nil
	}
	node.SetParent(nil)
	node.SetNext(nil)
}

// GraphFree frees a query graph.
func GraphFree(graph *Fork) {
	if graph == nil {
		return
	}
	GraphFreeRecursive(graph)
	graph.Graph = nil
	graph.SymTab = nil
	graph.Info = nil
}
