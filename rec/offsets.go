package rec

// OffsetsFixed builds a simple offsets array for fixed-length fields.
// The first element is the extra size and each subsequent element is the
// cumulative end offset including the extra bytes.
func OffsetsFixed(lengths []int, extra int) []int {
	if extra < 0 {
		extra = 0
	}
	offsets := make([]int, len(lengths)+1)
	offsets[0] = extra
	sum := extra
	for i, length := range lengths {
		if length < 0 {
			length = 0
		}
		sum += length
		offsets[i+1] = sum
	}
	return offsets
}
