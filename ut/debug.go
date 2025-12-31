package ut

// DebugInfo captures assertion context.
type DebugInfo struct {
	Expr string
	File string
	Line Ulint
}

// DbgStopThreads indicates debug assertions should stop threads.
var DbgStopThreads bool

// LastAssertion records the most recent assertion failure.
var LastAssertion DebugInfo

// LastStop records the most recent stop request.
var LastStop DebugInfo

// DbgAssertionFailed records a failed assertion.
func DbgAssertionFailed(expr, file string, line Ulint) {
	LastAssertion = DebugInfo{Expr: expr, File: file, Line: line}
	DbgStopThreads = true
}

// DbgStopThread records a request to stop the current thread.
func DbgStopThread(file string, line Ulint) {
	LastStop = DebugInfo{File: file, Line: line}
}

// DbgReset clears debug state.
func DbgReset() {
	DbgStopThreads = false
	LastAssertion = DebugInfo{}
	LastStop = DebugInfo{}
}
