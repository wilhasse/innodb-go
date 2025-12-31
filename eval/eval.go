package eval

import (
	"bytes"
	"errors"
)

// ValueKind describes the type of a value.
type ValueKind int

const (
	KindNull ValueKind = iota
	KindInt
	KindBool
	KindBytes
)

// Value holds an evaluated value.
type Value struct {
	Kind  ValueKind
	Int   int64
	Bool  bool
	Bytes []byte
}

// NodeType identifies a query graph node.
type NodeType int

const (
	NodeUnknown NodeType = iota
	NodeIf
	NodeElsif
	NodeWhile
	NodeFor
	NodeExit
	NodeReturn
	NodeAssignment
	NodeProc
	NodeSymbol
)

// NodeRef provides access to the embedded node data.
type NodeRef interface {
	Base() *Node
}

// Node represents a simple evaluation node.
type Node struct {
	Type   NodeType
	Parent NodeRef
	Next   NodeRef
	Val    Value
}

// Base returns the embedded node data.
func (n *Node) Base() *Node {
	if n == nil {
		return nil
	}
	return n
}

// NodeAllocValBuf allocates a buffer for node value bytes.
func NodeAllocValBuf(node NodeRef, size int) []byte {
	base := nodeBase(node)
	if base == nil {
		return nil
	}
	if size <= 0 {
		base.Val.Bytes = nil
		base.Val.Kind = KindBytes
		return nil
	}
	base.Val.Bytes = make([]byte, size)
	base.Val.Kind = KindBytes
	return base.Val.Bytes
}

// NodeFreeValBuf releases any allocated buffer.
func NodeFreeValBuf(node NodeRef) {
	base := nodeBase(node)
	if base == nil {
		return
	}
	base.Val.Bytes = nil
	if base.Val.Kind == KindBytes {
		base.Val.Kind = KindNull
	}
}

// EvalCmp evaluates a comparison between two values.
func EvalCmp(op string, left, right Value) (bool, error) {
	cmp, err := compareValues(left, right)
	if err != nil {
		return false, err
	}
	switch op {
	case "=":
		return cmp == 0, nil
	case "!=":
		return cmp != 0, nil
	case "<":
		return cmp < 0, nil
	case "<=":
		return cmp <= 0, nil
	case ">":
		return cmp > 0, nil
	case ">=":
		return cmp >= 0, nil
	default:
		return false, errors.New("eval: unsupported comparison")
	}
}

// EvalLogical evaluates logical operations.
func EvalLogical(op string, args ...Value) (bool, error) {
	switch op {
	case "NOT":
		if len(args) != 1 {
			return false, errors.New("eval: NOT requires one arg")
		}
		return !args[0].Bool, nil
	case "AND":
		if len(args) != 2 {
			return false, errors.New("eval: AND requires two args")
		}
		return args[0].Bool && args[1].Bool, nil
	case "OR":
		if len(args) != 2 {
			return false, errors.New("eval: OR requires two args")
		}
		return args[0].Bool || args[1].Bool, nil
	default:
		return false, errors.New("eval: unsupported logical op")
	}
}

// EvalArith evaluates integer arithmetic.
func EvalArith(op string, left, right Value) (int64, error) {
	if left.Kind != KindInt || right.Kind != KindInt {
		return 0, errors.New("eval: arithmetic requires int operands")
	}
	switch op {
	case "+":
		return left.Int + right.Int, nil
	case "-":
		return left.Int - right.Int, nil
	case "*":
		return left.Int * right.Int, nil
	case "/":
		if right.Int == 0 {
			return 0, errors.New("eval: divide by zero")
		}
		return left.Int / right.Int, nil
	case "%":
		if right.Int == 0 {
			return 0, errors.New("eval: modulo by zero")
		}
		return left.Int % right.Int, nil
	default:
		return 0, errors.New("eval: unsupported arithmetic op")
	}
}

// EvalSubstr returns a substring slice.
func EvalSubstr(input []byte, pos, length int) []byte {
	if pos < 0 {
		pos = 0
	}
	if length < 0 {
		length = 0
	}
	if pos >= len(input) {
		return nil
	}
	end := pos + length
	if end > len(input) {
		end = len(input)
	}
	return append([]byte(nil), input[pos:end]...)
}

// EvalConcat concatenates byte slices.
func EvalConcat(parts ...[]byte) []byte {
	var total int
	for _, p := range parts {
		total += len(p)
	}
	out := make([]byte, 0, total)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// EvalInstr returns the 1-based position of needle in haystack, or 0 if not found.
func EvalInstr(haystack, needle []byte) int {
	if len(needle) == 0 {
		return 1
	}
	idx := bytes.Index(haystack, needle)
	if idx == -1 {
		return 0
	}
	return idx + 1
}

func nodeBase(node NodeRef) *Node {
	if node == nil {
		return nil
	}
	return node.Base()
}

func compareValues(left, right Value) (int, error) {
	if left.Kind != right.Kind {
		return 0, errors.New("eval: mismatched value kinds")
	}
	switch left.Kind {
	case KindInt:
		switch {
		case left.Int < right.Int:
			return -1, nil
		case left.Int > right.Int:
			return 1, nil
		default:
			return 0, nil
		}
	case KindBool:
		switch {
		case !left.Bool && right.Bool:
			return -1, nil
		case left.Bool && !right.Bool:
			return 1, nil
		default:
			return 0, nil
		}
	case KindBytes:
		return bytes.Compare(left.Bytes, right.Bytes), nil
	case KindNull:
		return 0, nil
	default:
		return 0, errors.New("eval: unsupported value kind")
	}
}
