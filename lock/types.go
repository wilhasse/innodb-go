package lock

// Mode mirrors lock_mode in the C sources.
type Mode int

const (
	ModeIS Mode = iota
	ModeIX
	ModeS
	ModeX
)

const (
	ModeShared    = ModeS
	ModeExclusive = ModeX
)
