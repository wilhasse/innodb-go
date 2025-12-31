package mach

import (
	"encoding/binary"
	"math"

	"github.com/wilhasse/innodb-go/ut"
)

// WriteTo1 stores a single byte.
func WriteTo1(b []byte, n uint32) {
	if len(b) < 1 {
		return
	}
	b[0] = byte(n)
}

// ReadFrom1 reads a single byte.
func ReadFrom1(b []byte) uint32 {
	if len(b) < 1 {
		return 0
	}
	return uint32(b[0])
}

// WriteTo2 stores a 2-byte big-endian integer.
func WriteTo2(b []byte, n uint32) {
	if len(b) < 2 {
		return
	}
	b[0] = byte(n >> 8)
	b[1] = byte(n)
}

// ReadFrom2 reads a 2-byte big-endian integer.
func ReadFrom2(b []byte) uint32 {
	if len(b) < 2 {
		return 0
	}
	return uint32(b[0])<<8 | uint32(b[1])
}

// Encode2 stores a 16-bit value in canonical byte order for comparisons.
func Encode2(n uint32) uint16 {
	var buf [2]byte
	buf[0] = byte(n >> 8)
	buf[1] = byte(n)
	return ut.NativeEndian.Uint16(buf[:])
}

// Decode2 converts a canonical 16-bit value back to host order.
func Decode2(n uint16) uint32 {
	var buf [2]byte
	ut.NativeEndian.PutUint16(buf[:], n)
	return ReadFrom2(buf[:])
}

// WriteTo3 stores a 3-byte big-endian integer.
func WriteTo3(b []byte, n uint32) {
	if len(b) < 3 {
		return
	}
	b[0] = byte(n >> 16)
	b[1] = byte(n >> 8)
	b[2] = byte(n)
}

// ReadFrom3 reads a 3-byte big-endian integer.
func ReadFrom3(b []byte) uint32 {
	if len(b) < 3 {
		return 0
	}
	return uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
}

// WriteTo4 stores a 4-byte big-endian integer.
func WriteTo4(b []byte, n uint32) {
	if len(b) < 4 {
		return
	}
	b[0] = byte(n >> 24)
	b[1] = byte(n >> 16)
	b[2] = byte(n >> 8)
	b[3] = byte(n)
}

// ReadFrom4 reads a 4-byte big-endian integer.
func ReadFrom4(b []byte) uint32 {
	if len(b) < 4 {
		return 0
	}
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

// WriteCompressed stores a 32-bit integer using compressed encoding.
func WriteCompressed(b []byte, n uint32) int {
	switch {
	case n < 0x80:
		if len(b) < 1 {
			return 0
		}
		WriteTo1(b, n)
		return 1
	case n < 0x4000:
		if len(b) < 2 {
			return 0
		}
		WriteTo2(b, n|0x8000)
		return 2
	case n < 0x200000:
		if len(b) < 3 {
			return 0
		}
		WriteTo3(b, n|0xC00000)
		return 3
	case n < 0x10000000:
		if len(b) < 4 {
			return 0
		}
		WriteTo4(b, n|0xE0000000)
		return 4
	default:
		if len(b) < 5 {
			return 0
		}
		WriteTo1(b, 0xF0)
		WriteTo4(b[1:], n)
		return 5
	}
}

// GetCompressedSize returns the encoded size of a compressed integer.
func GetCompressedSize(n uint32) int {
	switch {
	case n < 0x80:
		return 1
	case n < 0x4000:
		return 2
	case n < 0x200000:
		return 3
	case n < 0x10000000:
		return 4
	default:
		return 5
	}
}

// ReadCompressed reads a compressed integer.
func ReadCompressed(b []byte) uint32 {
	if len(b) < 1 {
		return 0
	}
	flag := ReadFrom1(b)
	switch {
	case flag < 0x80:
		return flag
	case flag < 0xC0:
		if len(b) < 2 {
			return 0
		}
		return ReadFrom2(b) & 0x7FFF
	case flag < 0xE0:
		if len(b) < 3 {
			return 0
		}
		return ReadFrom3(b) & 0x3FFFFF
	case flag < 0xF0:
		if len(b) < 4 {
			return 0
		}
		return ReadFrom4(b) & 0x1FFFFFFF
	default:
		if len(b) < 5 {
			return 0
		}
		return ReadFrom4(b[1:])
	}
}

// WriteTo6 stores a 6-byte big-endian dulint.
func WriteTo6(b []byte, n ut.Dulint) {
	if len(b) < 6 {
		return
	}
	WriteTo2(b, dulintHigh(n))
	WriteTo4(b[2:], dulintLow(n))
}

// ReadFrom6 reads a 6-byte big-endian dulint.
func ReadFrom6(b []byte) ut.Dulint {
	if len(b) < 6 {
		return ut.Dulint{}
	}
	high := ReadFrom2(b)
	low := ReadFrom4(b[2:])
	return dulintCreate(high, low)
}

// WriteTo7 stores a 7-byte big-endian dulint.
func WriteTo7(b []byte, n ut.Dulint) {
	if len(b) < 7 {
		return
	}
	WriteTo3(b, dulintHigh(n))
	WriteTo4(b[3:], dulintLow(n))
}

// ReadFrom7 reads a 7-byte big-endian dulint.
func ReadFrom7(b []byte) ut.Dulint {
	if len(b) < 7 {
		return ut.Dulint{}
	}
	high := ReadFrom3(b)
	low := ReadFrom4(b[3:])
	return dulintCreate(high, low)
}

// WriteTo8 stores an 8-byte big-endian dulint.
func WriteTo8(b []byte, n ut.Dulint) {
	if len(b) < 8 {
		return
	}
	WriteTo4(b, dulintHigh(n))
	WriteTo4(b[4:], dulintLow(n))
}

// WriteUll stores a 64-bit integer in big-endian order.
func WriteUll(b []byte, n uint64) {
	if len(b) < 8 {
		return
	}
	WriteTo4(b, uint32(n>>32))
	WriteTo4(b[4:], uint32(n))
}

// ReadFrom8 reads an 8-byte big-endian dulint.
func ReadFrom8(b []byte) ut.Dulint {
	if len(b) < 8 {
		return ut.Dulint{}
	}
	high := ReadFrom4(b)
	low := ReadFrom4(b[4:])
	return dulintCreate(high, low)
}

// ReadUll reads a 64-bit integer in big-endian order.
func ReadUll(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return uint64(ReadFrom4(b))<<32 | uint64(ReadFrom4(b[4:]))
}

// DulintWriteCompressed writes a dulint using compressed encoding.
func DulintWriteCompressed(b []byte, n ut.Dulint) int {
	high := dulintHigh(n)
	low := dulintLow(n)
	size := GetCompressedSize(high)
	if len(b) < size+4 {
		return 0
	}
	WriteCompressed(b, high)
	WriteTo4(b[size:], low)
	return size + 4
}

// DulintGetCompressedSize returns the size of a compressed dulint.
func DulintGetCompressedSize(n ut.Dulint) int {
	return 4 + GetCompressedSize(dulintHigh(n))
}

// DulintReadCompressed reads a dulint encoded with compressed encoding.
func DulintReadCompressed(b []byte) ut.Dulint {
	if len(b) == 0 {
		return ut.Dulint{}
	}
	high := ReadCompressed(b)
	size := GetCompressedSize(high)
	if len(b) < size+4 {
		return ut.Dulint{}
	}
	low := ReadFrom4(b[size:])
	return dulintCreate(high, low)
}

// DulintWriteMuchCompressed writes a dulint in much-compressed form.
func DulintWriteMuchCompressed(b []byte, n ut.Dulint) int {
	high := dulintHigh(n)
	low := dulintLow(n)
	if high == 0 {
		return WriteCompressed(b, low)
	}
	sizeHigh := GetCompressedSize(high)
	sizeLow := GetCompressedSize(low)
	total := 1 + sizeHigh + sizeLow
	if len(b) < total {
		return 0
	}
	b[0] = 0xFF
	WriteCompressed(b[1:], high)
	WriteCompressed(b[1+sizeHigh:], low)
	return total
}

// DulintGetMuchCompressedSize returns the size of a much-compressed dulint.
func DulintGetMuchCompressedSize(n ut.Dulint) int {
	high := dulintHigh(n)
	if high == 0 {
		return GetCompressedSize(dulintLow(n))
	}
	return 1 + GetCompressedSize(high) + GetCompressedSize(dulintLow(n))
}

// DulintReadMuchCompressed reads a dulint encoded with much-compressed form.
func DulintReadMuchCompressed(b []byte) ut.Dulint {
	if len(b) == 0 {
		return ut.Dulint{}
	}
	var high uint32
	size := 0
	if b[0] == 0xFF {
		high = ReadCompressed(b[1:])
		size = 1 + GetCompressedSize(high)
		if len(b) < size {
			return ut.Dulint{}
		}
	}
	low := ReadCompressed(b[size:])
	return dulintCreate(high, low)
}

// ParseCompressed reads a compressed integer if fully contained.
func ParseCompressed(buf []byte) ([]byte, uint32, bool) {
	if len(buf) < 1 {
		return nil, 0, false
	}
	flag := ReadFrom1(buf)
	switch {
	case flag < 0x80:
		return buf[1:], flag, true
	case flag < 0xC0:
		if len(buf) < 2 {
			return nil, 0, false
		}
		return buf[2:], ReadFrom2(buf) & 0x7FFF, true
	case flag < 0xE0:
		if len(buf) < 3 {
			return nil, 0, false
		}
		return buf[3:], ReadFrom3(buf) & 0x3FFFFF, true
	case flag < 0xF0:
		if len(buf) < 4 {
			return nil, 0, false
		}
		return buf[4:], ReadFrom4(buf) & 0x1FFFFFFF, true
	default:
		if len(buf) < 5 {
			return nil, 0, false
		}
		return buf[5:], ReadFrom4(buf[1:]), true
	}
}

// DulintParseCompressed reads a compressed dulint if fully contained.
func DulintParseCompressed(buf []byte) ([]byte, ut.Dulint, bool) {
	rest, high, ok := ParseCompressed(buf)
	if !ok || len(rest) < 4 {
		return nil, ut.Dulint{}, false
	}
	low := ReadFrom4(rest)
	return rest[4:], dulintCreate(high, low), true
}

// DoubleRead reads a float64 stored in little-endian order.
func DoubleRead(b []byte) float64 {
	if len(b) < 8 {
		return 0
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(b))
}

// DoublePtrWrite writes a float64 byte representation in little-endian order.
func DoublePtrWrite(dest []byte, src []byte) {
	if len(dest) < 8 || len(src) < 8 {
		return
	}
	if ut.NativeEndian == binary.BigEndian {
		for i := 0; i < 8; i++ {
			dest[i] = src[7-i]
		}
		return
	}
	copy(dest, src[:8])
}

// DoubleWrite writes a float64 in little-endian order.
func DoubleWrite(dest []byte, v float64) {
	if len(dest) < 8 {
		return
	}
	binary.LittleEndian.PutUint64(dest, math.Float64bits(v))
}

// FloatRead reads a float32 stored in little-endian order.
func FloatRead(b []byte) float32 {
	if len(b) < 4 {
		return 0
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(b))
}

// FloatPtrWrite writes a float32 byte representation in little-endian order.
func FloatPtrWrite(dest []byte, src []byte) {
	if len(dest) < 4 || len(src) < 4 {
		return
	}
	if ut.NativeEndian == binary.BigEndian {
		for i := 0; i < 4; i++ {
			dest[i] = src[3-i]
		}
		return
	}
	copy(dest, src[:4])
}

// FloatWrite writes a float32 in little-endian order.
func FloatWrite(dest []byte, v float32) {
	if len(dest) < 4 {
		return
	}
	binary.LittleEndian.PutUint32(dest, math.Float32bits(v))
}

// ReadFromNLittleEndian reads a value from little-endian bytes.
func ReadFromNLittleEndian(buf []byte, size int) uint64 {
	if size <= 0 || len(buf) < size {
		return 0
	}
	var n uint64
	for i := size - 1; i >= 0; i-- {
		n = (n << 8) | uint64(buf[i])
	}
	return n
}

// WriteToNLittleEndian writes a value to little-endian bytes.
func WriteToNLittleEndian(dest []byte, size int, n uint64) {
	if size <= 0 || len(dest) < size {
		return
	}
	for i := 0; i < size; i++ {
		dest[i] = byte(n & 0xFF)
		n >>= 8
	}
}

// ReadFrom2LittleEndian reads a 2-byte little-endian integer.
func ReadFrom2LittleEndian(buf []byte) uint32 {
	if len(buf) < 2 {
		return 0
	}
	return uint32(buf[0]) | uint32(buf[1])<<8
}

// WriteTo2LittleEndian writes a 2-byte little-endian integer.
func WriteTo2LittleEndian(dest []byte, n uint32) {
	if len(dest) < 2 {
		return
	}
	dest[0] = byte(n & 0xFF)
	dest[1] = byte((n >> 8) & 0xFF)
}

// ReadIntType converts an integer from storage order to host order.
func ReadIntType(dst []byte, src []byte, unsigned bool) {
	if len(dst) == 0 || len(src) < len(dst) {
		return
	}
	if ut.NativeEndian == binary.BigEndian {
		copy(dst, src[:len(dst)])
		return
	}
	swapByteOrder(dst, src[:len(dst)])
	if unsigned {
		dst[len(dst)-1] ^= 0x80
	}
}

// WriteIntType converts an integer from host order to storage order.
func WriteIntType(dest []byte, src []byte, unsigned bool) {
	if len(dest) == 0 || len(src) < len(dest) {
		return
	}
	if ut.NativeEndian == binary.BigEndian {
		copy(dest, src[:len(dest)])
		return
	}
	swapByteOrder(dest, src[:len(dest)])
	if unsigned {
		dest[0] ^= 0x80
	}
}

// ReadUint64 reads a big-endian unsigned 64-bit integer with sign-bit encoding.
func ReadUint64(src []byte) uint64 {
	var buf [8]byte
	ReadIntType(buf[:], src, true)
	return ut.NativeEndian.Uint64(buf[:])
}

// ReadInt64 reads a big-endian signed 64-bit integer.
func ReadInt64(src []byte) int64 {
	var buf [8]byte
	ReadIntType(buf[:], src, false)
	return int64(ut.NativeEndian.Uint64(buf[:]))
}

// ReadUint32 reads a big-endian unsigned 32-bit integer with sign-bit encoding.
func ReadUint32(src []byte) uint32 {
	var buf [4]byte
	ReadIntType(buf[:], src, true)
	return ut.NativeEndian.Uint32(buf[:])
}

// ReadInt32 reads a big-endian signed 32-bit integer.
func ReadInt32(src []byte) int32 {
	var buf [4]byte
	ReadIntType(buf[:], src, false)
	return int32(ut.NativeEndian.Uint32(buf[:]))
}

// WriteUint64 writes a big-endian unsigned 64-bit integer with sign-bit encoding.
func WriteUint64(dest []byte, n uint64) {
	var buf [8]byte
	ut.NativeEndian.PutUint64(buf[:], n)
	WriteIntType(dest, buf[:], true)
}

// WriteInt64 writes a big-endian signed 64-bit integer.
func WriteInt64(dest []byte, n int64) {
	var buf [8]byte
	ut.NativeEndian.PutUint64(buf[:], uint64(n))
	WriteIntType(dest, buf[:], false)
}

// WriteUint32 writes a big-endian unsigned 32-bit integer with sign-bit encoding.
func WriteUint32(dest []byte, n uint32) {
	var buf [4]byte
	ut.NativeEndian.PutUint32(buf[:], n)
	WriteIntType(dest, buf[:], true)
}

// WriteInt32 writes a big-endian signed 32-bit integer.
func WriteInt32(dest []byte, n int32) {
	var buf [4]byte
	ut.NativeEndian.PutUint32(buf[:], uint32(n))
	WriteIntType(dest, buf[:], false)
}

func dulintCreate(high, low uint32) ut.Dulint {
	return ut.Dulint{High: ut.Ulint(high), Low: ut.Ulint(low)}
}

func dulintHigh(n ut.Dulint) uint32 {
	return uint32(n.High)
}

func dulintLow(n ut.Dulint) uint32 {
	return uint32(n.Low)
}

func swapByteOrder(dest []byte, src []byte) {
	for i := 0; i < len(dest); i++ {
		dest[len(dest)-1-i] = src[i]
	}
}
