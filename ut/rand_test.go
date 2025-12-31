package ut

import "testing"

func TestRandSeedDeterminism(t *testing.T) {
	RandSetSeed(1234)
	a := RandGenUlint()
	RandSetSeed(1234)
	b := RandGenUlint()
	if a != b {
		t.Fatalf("expected deterministic sequence")
	}
}

func TestRandIntervalAndBool(t *testing.T) {
	RandSetSeed(1)
	val := RandInterval(5, 10)
	if val < 5 || val > 10 {
		t.Fatalf("val=%d", val)
	}
	b := RandGenIBool()
	if b != 0 && b != 1 {
		t.Fatalf("bool=%d", b)
	}
}

func TestHashAndFold(t *testing.T) {
	if got := HashUlint(10, 7); got >= 7 {
		t.Fatalf("hash=%d", got)
	}
	if FoldString("abc") != FoldBinary([]byte("abc")) {
		t.Fatalf("fold mismatch")
	}
}

func TestFindPrime(t *testing.T) {
	p := FindPrime(200)
	if p <= 200 {
		t.Fatalf("prime=%d", p)
	}
	if !isPrime(p) {
		t.Fatalf("not prime: %d", p)
	}
}
