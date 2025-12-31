package ut

// DulintZero is the zero value for a Dulint.
var DulintZero = Dulint{High: 0, Low: 0}

// DulintMax is the maximum value for a Dulint.
var DulintMax = Dulint{High: ^Ulint(0), Low: ^Ulint(0)}

// DulintCreate builds a Dulint from high and low parts.
func DulintCreate(high, low Ulint) Dulint {
	return Dulint{High: high, Low: low}
}

// DulintHigh returns the high word.
func DulintHigh(d Dulint) Ulint {
	return d.High
}

// DulintLow returns the low word.
func DulintLow(d Dulint) Ulint {
	return d.Low
}

// DulintIsZero reports whether the dulint is zero.
func DulintIsZero(d Dulint) bool {
	return d.High == 0 && d.Low == 0
}

// DulintCmp compares two dulints.
func DulintCmp(a, b Dulint) int {
	switch {
	case a.High < b.High:
		return -1
	case a.High > b.High:
		return 1
	case a.Low < b.Low:
		return -1
	case a.Low > b.Low:
		return 1
	default:
		return 0
	}
}

// DulintAdd adds a low-word value to a dulint.
func DulintAdd(a Dulint, b Ulint) Dulint {
	sumLow := a.Low + b
	carry := Ulint(0)
	if sumLow < a.Low {
		carry = 1
	}
	return Dulint{High: a.High + carry, Low: sumLow}
}

// DulintSubtract subtracts a low-word value from a dulint.
func DulintSubtract(a Dulint, b Ulint) Dulint {
	if a.Low >= b {
		return Dulint{High: a.High, Low: a.Low - b}
	}
	return Dulint{High: a.High - 1, Low: a.Low - b}
}

// DulintAlignDown rounds a dulint down to the nearest aligned value.
func DulintAlignDown(n Dulint, align Ulint) Dulint {
	if align == 0 {
		return n
	}
	mask := align - 1
	return Dulint{High: n.High, Low: n.Low & ^mask}
}

// DulintAlignUp rounds a dulint up to the nearest aligned value.
func DulintAlignUp(n Dulint, align Ulint) Dulint {
	if align == 0 {
		return n
	}
	mask := align - 1
	if n.Low&mask == 0 {
		return n
	}
	low := (n.Low + align) & ^mask
	high := n.High
	if low < n.Low {
		high++
	}
	return Dulint{High: high, Low: low}
}

// Uint64AlignDown rounds a uint64 down to the nearest aligned value.
func Uint64AlignDown(n uint64, align Ulint) uint64 {
	if align == 0 {
		return n
	}
	mask := uint64(align - 1)
	return n & ^mask
}

// Uint64AlignUp rounds a uint64 up to the nearest aligned value.
func Uint64AlignUp(n uint64, align Ulint) uint64 {
	if align == 0 {
		return n
	}
	mask := uint64(align - 1)
	return (n + mask) & ^mask
}
