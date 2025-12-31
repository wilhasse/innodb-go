package rec

// InfimumExtra and SupremumExtra mirror the compact record header bytes
// used for the special infimum/supremum records. The next pointer bytes
// are zeroed and should be filled by callers when installing on a page.
var (
	InfimumExtra  = []byte{0x01, 0x00, 0x02, 0x00, 0x00}
	InfimumData   = []byte{'i', 'n', 'f', 'i', 'm', 'u', 'm', 0x00}
	SupremumExtra = []byte{0x01, 0x00, 0x0b, 0x00, 0x00}
	SupremumData  = []byte{'s', 'u', 'p', 'r', 'e', 'm', 'u', 'm'}
)
