package rem

// RecordType identifies the logical record type on a page.
type RecordType int

const (
	RecordUser RecordType = iota
	RecordInfimum
	RecordSupremum
)

// Heap number constants for system records.
const (
	HeapNoInfimum  uint16 = 0
	HeapNoSupremum uint16 = 1
)
