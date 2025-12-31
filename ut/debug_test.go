package ut

import "testing"

func TestDbgAssertionFailed(t *testing.T) {
	DbgReset()
	DbgAssertionFailed("x > 0", "file.go", 42)
	if !DbgStopThreads {
		t.Fatalf("expected stop flag")
	}
	if LastAssertion.Expr != "x > 0" || LastAssertion.File != "file.go" || LastAssertion.Line != 42 {
		t.Fatalf("assertion=%v", LastAssertion)
	}
}

func TestDbgStopThread(t *testing.T) {
	DbgReset()
	DbgStopThread("other.go", 10)
	if LastStop.File != "other.go" || LastStop.Line != 10 {
		t.Fatalf("stop=%v", LastStop)
	}
}
