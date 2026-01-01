package lock

// ModeCompatible reports whether two lock modes are compatible.
func ModeCompatible(a, b Mode) bool {
	if !modeValid(a) || !modeValid(b) {
		return false
	}
	return modeCompat[a][b]
}

// ModeStrongerOrEq reports whether a is stronger than or equal to b.
func ModeStrongerOrEq(a, b Mode) bool {
	if !modeValid(a) || !modeValid(b) {
		return false
	}
	return a >= b
}

// ModeName returns a short string for the mode.
func ModeName(mode Mode) string {
	switch mode {
	case ModeIS:
		return "IS"
	case ModeIX:
		return "IX"
	case ModeS:
		return "S"
	case ModeX:
		return "X"
	default:
		return "UNKNOWN"
	}
}

func modeValid(mode Mode) bool {
	return mode >= ModeIS && mode <= ModeX
}

var modeCompat = [][]bool{
	// IS   IX    S     X
	{true, true, true, false},  // IS
	{true, true, false, false}, // IX
	{true, false, true, false}, // S
	{false, false, false, false}, // X
}
