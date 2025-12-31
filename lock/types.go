package lock

// Mode mirrors lock_mode in the C sources.
type Mode int

const (
	ModeShared Mode = iota
	ModeExclusive
)
