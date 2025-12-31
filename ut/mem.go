package ut

// MemTotalAllocated tracks bytes allocated via ut helpers.
var MemTotalAllocated Ulint

// MemVarInit resets memory tracking.
func MemVarInit() {
	MemTotalAllocated = 0
}

// MemInit initializes memory tracking.
func MemInit() {
}

// MallocLow allocates a byte slice with optional zeroing.
func MallocLow(n Ulint, setToZero bool, _ bool) []byte {
	if n == 0 {
		return nil
	}
	buf := make([]byte, n)
	if !setToZero {
		// Leave contents unspecified by skipping explicit zeroing.
	}
	MemTotalAllocated += n
	return buf
}

// Malloc allocates a zeroed byte slice.
func Malloc(n Ulint) []byte {
	return MallocLow(n, true, true)
}

// Free releases memory and updates counters.
func Free(buf []byte) {
	if buf == nil {
		return
	}
	MemTotalAllocated -= Ulint(len(buf))
}

// TestMalloc reports whether an allocation of n bytes succeeds.
func TestMalloc(n Ulint) bool {
	defer func() {
		_ = recover()
	}()
	buf := make([]byte, n)
	return buf != nil
}

// Realloc resizes a buffer, copying the previous contents.
func Realloc(buf []byte, size Ulint) []byte {
	if size == 0 {
		Free(buf)
		return nil
	}
	newBuf := make([]byte, size)
	copy(newBuf, buf)
	if buf != nil {
		MemTotalAllocated -= Ulint(len(buf))
	}
	MemTotalAllocated += size
	return newBuf
}

// FreeAllMem resets the allocation counter.
func FreeAllMem() {
	MemTotalAllocated = 0
}

// Memcpy copies n bytes from src to dst.
func Memcpy(dst, src []byte, n Ulint) []byte {
	if n == 0 {
		return dst
	}
	if int(n) > len(src) {
		n = Ulint(len(src))
	}
	if int(n) > len(dst) {
		n = Ulint(len(dst))
	}
	copy(dst[:n], src[:n])
	return dst
}

// Memmove copies n bytes allowing overlap.
func Memmove(dst, src []byte, n Ulint) []byte {
	return Memcpy(dst, src, n)
}

// Memcmp compares the first n bytes of two slices.
func Memcmp(a, b []byte, n Ulint) int {
	if int(n) > len(a) {
		n = Ulint(len(a))
	}
	if int(n) > len(b) {
		n = Ulint(len(b))
	}
	for i := 0; i < int(n); i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

// Strcpy copies a string into a byte buffer.
func Strcpy(dst []byte, src string) []byte {
	copy(dst, src)
	return dst
}

// Strlen returns the length of a string in bytes.
func Strlen(s string) Ulint {
	return Ulint(len(s))
}

// Strcmp compares two strings.
func Strcmp(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// Strlcpy copies src into dst and NUL-terminates when possible.
func Strlcpy(dst []byte, src string) Ulint {
	if len(dst) == 0 {
		return Ulint(len(src))
	}
	n := len(dst) - 1
	if n > len(src) {
		n = len(src)
	}
	copy(dst[:n], src[:n])
	dst[n] = 0
	return Ulint(len(src))
}
