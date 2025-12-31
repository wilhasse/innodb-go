package eval

import "testing"

func TestEvalCmpInt(t *testing.T) {
	left := Value{Kind: KindInt, Int: 10}
	right := Value{Kind: KindInt, Int: 20}
	ok, err := EvalCmp("<", left, right)
	if err != nil || !ok {
		t.Fatalf("expected comparison to be true")
	}
	ok, err = EvalCmp(">=", left, right)
	if err != nil || ok {
		t.Fatalf("expected comparison to be false")
	}
}

func TestEvalLogical(t *testing.T) {
	a := Value{Kind: KindBool, Bool: true}
	b := Value{Kind: KindBool, Bool: false}
	ok, err := EvalLogical("AND", a, b)
	if err != nil || ok {
		t.Fatalf("expected AND to be false")
	}
	ok, err = EvalLogical("OR", a, b)
	if err != nil || !ok {
		t.Fatalf("expected OR to be true")
	}
	ok, err = EvalLogical("NOT", b)
	if err != nil || !ok {
		t.Fatalf("expected NOT to be true")
	}
}

func TestEvalArith(t *testing.T) {
	left := Value{Kind: KindInt, Int: 7}
	right := Value{Kind: KindInt, Int: 3}
	val, err := EvalArith("+", left, right)
	if err != nil || val != 10 {
		t.Fatalf("expected 10, got %d", val)
	}
	val, err = EvalArith("%", left, right)
	if err != nil || val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

func TestEvalSubstrConcatInstr(t *testing.T) {
	sub := EvalSubstr([]byte("abcdef"), 2, 3)
	if string(sub) != "cde" {
		t.Fatalf("unexpected substring: %s", sub)
	}
	concat := EvalConcat([]byte("ab"), []byte("cd"))
	if string(concat) != "abcd" {
		t.Fatalf("unexpected concat result: %s", concat)
	}
	if pos := EvalInstr([]byte("hello"), []byte("ll")); pos != 3 {
		t.Fatalf("unexpected instr result: %d", pos)
	}
}

func TestNodeAllocFreeValBuf(t *testing.T) {
	node := &Node{}
	buf := NodeAllocValBuf(node, 4)
	if buf == nil || len(buf) != 4 {
		t.Fatalf("expected buffer size 4")
	}
	NodeFreeValBuf(node)
	if node.Val.Bytes != nil {
		t.Fatalf("expected buffer to be freed")
	}
}
