package ut

import (
	"encoding/binary"
	"unsafe"
)

// NativeEndian reflects the host byte order for packing/unpacking.
var NativeEndian = detectNativeEndian()

// Dulint mirrors the C dulint struct used for 64-bit values on 32-bit builds.
type Dulint struct {
	High Ulint
	Low  Ulint
}

func detectNativeEndian() binary.ByteOrder {
	var x uint16 = 0x1
	b := *(*[2]byte)(unsafe.Pointer(&x))
	if b[0] == 0x1 {
		return binary.LittleEndian
	}
	return binary.BigEndian
}

func init() {
	if unsafe.Sizeof(Ulint(0)) != unsafe.Sizeof(uintptr(0)) {
		panic("ut: Ulint size must match uintptr size")
	}
	if unsafe.Alignof(Ulint(0)) != unsafe.Alignof(uintptr(0)) {
		panic("ut: Ulint alignment must match uintptr alignment")
	}
	if unsafe.Sizeof(Dulint{}) != 2*unsafe.Sizeof(Ulint(0)) {
		panic("ut: Dulint size must be two Ulint words")
	}
}
