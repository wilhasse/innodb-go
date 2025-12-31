package dict

import "github.com/wilhasse/innodb-go/ut"

// DulintToUint64 converts a Dulint to uint64.
func DulintToUint64(d ut.Dulint) uint64 {
	return dulintToUint64(d)
}

// DulintFromUint64 converts a uint64 to Dulint.
func DulintFromUint64(value uint64) ut.Dulint {
	return newDulint(uint32(value>>32), uint32(value))
}
