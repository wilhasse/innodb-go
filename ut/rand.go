package ut

const (
	randMul1 = 1664525
	randAdd1 = 1013904223
)

// RandUlintCounter is the seed for the pseudo-random generator.
var RandUlintCounter Ulint = 65654363

// RandSetSeed sets the random seed.
func RandSetSeed(seed Ulint) {
	RandUlintCounter = seed
}

// RandGenNextUlint returns the next pseudo-random value from rnd.
func RandGenNextUlint(rnd Ulint) Ulint {
	return rnd*randMul1 + randAdd1
}

// RandGenUlint generates a pseudo-random ulint.
func RandGenUlint() Ulint {
	RandUlintCounter = RandGenNextUlint(RandUlintCounter)
	return RandUlintCounter
}

// RandInterval returns a random number in [low, high].
func RandInterval(low, high Ulint) Ulint {
	if high < low {
		low, high = high, low
	}
	if high == low {
		return low
	}
	return low + RandGenUlint()%(high-low+1)
}

// RandGenIBool returns a random boolean as IBool.
func RandGenIBool() IBool {
	if RandGenUlint()%2 == 0 {
		return 0
	}
	return 1
}

// HashUlint hashes a ulint to a table size.
func HashUlint(key, tableSize Ulint) Ulint {
	if tableSize == 0 {
		return 0
	}
	return key % tableSize
}

// FoldUlintPair folds a pair of ulints into one.
func FoldUlintPair(n1, n2 Ulint) Ulint {
	return n1*131 + n2
}

// FoldDulint folds a dulint into one.
func FoldDulint(d Dulint) Ulint {
	return FoldUlintPair(d.High, d.Low)
}

// FoldString folds a string into a hash value.
func FoldString(str string) Ulint {
	var h Ulint
	for i := 0; i < len(str); i++ {
		h = h*131 + Ulint(str[i])
	}
	return h
}

// FoldBinary folds a byte slice into a hash value.
func FoldBinary(buf []byte) Ulint {
	var h Ulint
	for _, b := range buf {
		h = h*131 + Ulint(b)
	}
	return h
}

// FindPrime returns a prime slightly greater than n.
func FindPrime(n Ulint) Ulint {
	n += 100
	pow2 := Ulint(1)
	for pow2*2 < n {
		pow2 *= 2
	}
	if float64(n) < 1.05*float64(pow2) {
		n = Ulint(float64(n) * 1.0412321)
	}
	pow2 *= 2
	if float64(n) > 0.95*float64(pow2) {
		n = Ulint(float64(n) * 1.1131347)
	}
	if n > pow2-20 {
		n += 30
	}
	n = Ulint(float64(n) * 1.0132677)
	for {
		if isPrime(n) {
			return n
		}
		n++
	}
}

func isPrime(n Ulint) bool {
	if n < 2 {
		return false
	}
	for i := Ulint(2); i*i <= n; i++ {
		if n%i == 0 {
			return false
		}
	}
	return true
}
