package ut

import "testing"

func TestDulintZeroMax(t *testing.T) {
	if DulintZero.High != 0 || DulintZero.Low != 0 {
		t.Fatalf("zero=%v", DulintZero)
	}
	max := ^Ulint(0)
	if DulintMax.High != max || DulintMax.Low != max {
		t.Fatalf("max=%v", DulintMax)
	}
}

func TestDulintAddSubtractCmp(t *testing.T) {
	max := ^Ulint(0)
	d := Dulint{High: 1, Low: max}
	sum := DulintAdd(d, 1)
	if sum.High != 2 || sum.Low != 0 {
		t.Fatalf("sum=%v", sum)
	}
	diff := DulintSubtract(Dulint{High: 2, Low: 0}, 1)
	if diff.High != 1 || diff.Low != max {
		t.Fatalf("diff=%v", diff)
	}
	if DulintCmp(DulintZero, DulintMax) != -1 {
		t.Fatalf("expected zero < max")
	}
}

func TestDulintAlign(t *testing.T) {
	d := Dulint{High: 1, Low: 9}
	down := DulintAlignDown(d, 8)
	if down.Low != 8 || down.High != 1 {
		t.Fatalf("down=%v", down)
	}
	up := DulintAlignUp(d, 8)
	if up.Low != 16 || up.High != 1 {
		t.Fatalf("up=%v", up)
	}

	max := ^Ulint(0)
	overflow := DulintAlignUp(Dulint{High: 1, Low: max}, 2)
	if overflow.High != 2 || overflow.Low != 0 {
		t.Fatalf("overflow=%v", overflow)
	}
}

func TestUint64Align(t *testing.T) {
	if got := Uint64AlignDown(19, 8); got != 16 {
		t.Fatalf("down=%d", got)
	}
	if got := Uint64AlignUp(19, 8); got != 24 {
		t.Fatalf("up=%d", got)
	}
}
