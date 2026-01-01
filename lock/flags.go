package lock

// Flags describe lock state bits.
type Flags uint32

const (
	FlagWait Flags = 1 << iota
)
