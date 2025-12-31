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
	sum := 0
	for i, length := range lengths {
		if length < 0 {
			length = 0
		}
		sum += length
		offsets[i+1] = sum
	}
	return offsets
}

// OffsetsVar builds offsets for variable-length fields with NULL flags.
// offsets[0] stores the extra size and offsets[1:] store end offsets or
// end offsets OR'd with RecOffsSQLNull when the field is NULL.
func OffsetsVar(lengths []int, nulls []bool, extra int) []uint32 {
	if extra < 0 {
		extra = 0
	}
	offsets := make([]uint32, len(lengths)+1)
	offsets[0] = uint32(extra)
	var pos uint32
	for i, length := range lengths {
		if i < len(nulls) && nulls[i] {
			offsets[i+1] = pos | RecOffsSQLNull
			continue
		}
		if length < 0 {
			length = 0
		}
		pos += uint32(length)
		offsets[i+1] = pos
	}
	return offsets
}
