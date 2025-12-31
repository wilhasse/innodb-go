package eval

import "testing"

func TestIfStepTrueBranch(t *testing.T) {
	parent := &ProcNode{Node: Node{Type: NodeProc}}
	cond := &Node{Type: NodeSymbol, Val: Value{Kind: KindBool, Bool: true}}
	stat := &Node{}
	ifNode := &IfNode{Node: Node{Type: NodeIf, Parent: parent}, Cond: cond, StatList: stat}
	thr := &Thr{RunNode: ifNode, PrevNode: parent}

	ifStep(thr)
	if thr.RunNode != stat {
		t.Fatalf("expected stat list to run, got %v", thr.RunNode)
	}
}

func TestIfStepElsifBranch(t *testing.T) {
	parent := &ProcNode{Node: Node{Type: NodeProc}}
	cond := &Node{Type: NodeSymbol, Val: Value{Kind: KindBool, Bool: false}}
	elsif1 := &ElsifNode{Node: Node{Type: NodeElsif}, Cond: &Node{Val: Value{Kind: KindBool, Bool: false}}, StatList: &Node{}}
	elsif2Stat := &Node{}
	elsif2 := &ElsifNode{Node: Node{Type: NodeElsif}, Cond: &Node{Val: Value{Kind: KindBool, Bool: true}}, StatList: elsif2Stat}
	NodeSetNext(elsif1, elsif2)

	ifNode := &IfNode{Node: Node{Type: NodeIf, Parent: parent}, Cond: cond, ElsifList: elsif1}
	thr := &Thr{RunNode: ifNode, PrevNode: parent}

	ifStep(thr)
	if thr.RunNode != elsif2Stat {
		t.Fatalf("expected elsif stat list to run, got %v", thr.RunNode)
	}
}

func TestIfStepExitToParent(t *testing.T) {
	parent := &ProcNode{Node: Node{Type: NodeProc}}
	stat := &Node{}
	ifNode := &IfNode{Node: Node{Type: NodeIf, Parent: parent}, StatList: stat}
	thr := &Thr{RunNode: ifNode, PrevNode: stat}

	ifStep(thr)
	if thr.RunNode != parent {
		t.Fatalf("expected to return to parent, got %v", thr.RunNode)
	}
}

func TestWhileStep(t *testing.T) {
	parent := &ProcNode{Node: Node{Type: NodeProc}}
	cond := &Node{Val: Value{Kind: KindBool, Bool: false}}
	stat := &Node{}
	whileNode := &WhileNode{Node: Node{Type: NodeWhile, Parent: parent}, Cond: cond, StatList: stat}
	thr := &Thr{RunNode: whileNode, PrevNode: parent}

	whileStep(thr)
	if thr.RunNode != parent {
		t.Fatalf("expected to exit loop, got %v", thr.RunNode)
	}
}

func TestAssignStep(t *testing.T) {
	parent := &ProcNode{Node: Node{Type: NodeProc}}
	alias := &Symbol{Node: Node{Type: NodeSymbol}}
	variable := &Symbol{Node: Node{Type: NodeSymbol}, Alias: alias}
	val := &Node{Val: Value{Kind: KindInt, Int: 42}}
	assignNode := &AssignNode{Node: Node{Type: NodeAssignment, Parent: parent}, Var: variable, Val: val}
	thr := &Thr{RunNode: assignNode}

	assignStep(thr)
	if alias.Val.Int != 42 {
		t.Fatalf("expected alias value 42, got %d", alias.Val.Int)
	}
	if thr.RunNode != parent {
		t.Fatalf("expected to return to parent, got %v", thr.RunNode)
	}
}

func TestForStepLoop(t *testing.T) {
	parent := &ProcNode{Node: Node{Type: NodeProc}}
	loopVar := &Symbol{Node: Node{Type: NodeSymbol}}
	start := &Node{Val: Value{Kind: KindInt, Int: 1}}
	end := &Node{Val: Value{Kind: KindInt, Int: 3}}
	body := &Node{}
	forNode := &ForNode{Node: Node{Type: NodeFor, Parent: parent}, LoopVar: loopVar, LoopStartLimit: start, LoopEndLimit: end, StatList: body}
	thr := &Thr{}

	thr.RunNode = forNode
	thr.PrevNode = parent
	forStep(thr)
	if thr.RunNode != body || loopVar.Val.Int != 1 {
		t.Fatalf("expected loop start at 1")
	}

	thr.RunNode = forNode
	thr.PrevNode = body
	forStep(thr)
	if thr.RunNode != body || loopVar.Val.Int != 2 {
		t.Fatalf("expected loop to advance to 2")
	}

	thr.RunNode = forNode
	thr.PrevNode = body
	forStep(thr)
	if thr.RunNode != body || loopVar.Val.Int != 3 {
		t.Fatalf("expected loop to advance to 3")
	}

	thr.RunNode = forNode
	thr.PrevNode = body
	forStep(thr)
	if thr.RunNode != parent {
		t.Fatalf("expected loop exit to parent, got %v", thr.RunNode)
	}
}

func TestExitStep(t *testing.T) {
	root := &ProcNode{Node: Node{Type: NodeProc}}
	loop := &WhileNode{Node: Node{Type: NodeWhile, Parent: root}}
	inner := &Node{Parent: loop}
	exitNode := &ExitNode{Node: Node{Type: NodeExit, Parent: inner}}
	thr := &Thr{RunNode: exitNode}

	exitStep(thr)
	if thr.RunNode != root {
		t.Fatalf("expected exit to loop parent, got %v", thr.RunNode)
	}
}

func TestReturnStep(t *testing.T) {
	root := &Node{Type: NodeUnknown}
	proc := &ProcNode{Node: Node{Type: NodeProc, Parent: root}}
	inner := &Node{Parent: proc}
	ret := &ReturnNode{Node: Node{Type: NodeReturn, Parent: inner}}
	thr := &Thr{RunNode: ret}

	returnStep(thr)
	if thr.RunNode != root {
		t.Fatalf("expected return to proc parent, got %v", thr.RunNode)
	}
}
