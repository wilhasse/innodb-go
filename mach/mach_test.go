package mach

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/wilhasse/innodb-go/ut"
)

func TestReadWriteFixedWidths(t *testing.T) {
	buf := make([]byte, 8)

	WriteTo1(buf[:1], 0x7f)
	if got := ReadFrom1(buf[:1]); got != 0x7f {
		t.Fatalf("ReadFrom1=%#x", got)
	}

	WriteTo2(buf[:2], 0x1234)
	if got := ReadFrom2(buf[:2]); got != 0x1234 {
		t.Fatalf("ReadFrom2=%#x", got)
	}

	WriteTo3(buf[:3], 0xabcdef)
	if got := ReadFrom3(buf[:3]); got != 0xabcdef {
		t.Fatalf("ReadFrom3=%#x", got)
	}

	WriteTo4(buf[:4], 0x89abcdef)
	if got := ReadFrom4(buf[:4]); got != 0x89abcdef {
		t.Fatalf("ReadFrom4=%#x", got)
	}

	enc := Encode2(0x1234)
	var encBuf [2]byte
	ut.NativeEndian.PutUint16(encBuf[:], enc)
	if encBuf != [2]byte{0x12, 0x34} {
		t.Fatalf("Encode2 bytes=%#v", encBuf)
	}
	if got := Decode2(enc); got != 0x1234 {
		t.Fatalf("Decode2=%#x", got)
	}
}

func TestReadWriteDulintWidths(t *testing.T) {
	buf := make([]byte, 8)

	d6 := makeDulint(0x1234, 0x56789abc)
	WriteTo6(buf[:6], d6)
	if got := ReadFrom6(buf[:6]); got != d6 {
		t.Fatalf("ReadFrom6=%#v", got)
	}

	d7 := makeDulint(0x123456, 0x789abcde)
	WriteTo7(buf[:7], d7)
	if got := ReadFrom7(buf[:7]); got != d7 {
		t.Fatalf("ReadFrom7=%#v", got)
	}

	d8 := makeDulint(0x12345678, 0x9abcdef0)
	WriteTo8(buf[:8], d8)
	if got := ReadFrom8(buf[:8]); got != d8 {
		t.Fatalf("ReadFrom8=%#v", got)
	}

	ull := uint64(0x1122334455667788)
	WriteUll(buf[:8], ull)
	if got := ReadUll(buf[:8]); got != ull {
		t.Fatalf("ReadUll=%#x", got)
	}
}

func TestCompressedRoundTrip(t *testing.T) {
	cases := []struct {
		value uint32
		size  int
	}{
		{value: 0x7f, size: 1},
		{value: 0x80, size: 2},
		{value: 0x3fff, size: 2},
		{value: 0x4000, size: 3},
		{value: 0x1fffff, size: 3},
		{value: 0x200000, size: 4},
		{value: 0x0fffffff, size: 4},
		{value: 0x10000000, size: 5},
		{value: 0xffffffff, size: 5},
	}

	for _, tc := range cases {
		buf := make([]byte, 8)
		if got := GetCompressedSize(tc.value); got != tc.size {
			t.Fatalf("GetCompressedSize(%#x)=%d", tc.value, got)
		}
		if got := WriteCompressed(buf, tc.value); got != tc.size {
			t.Fatalf("WriteCompressed(%#x)=%d", tc.value, got)
		}
		if got := ReadCompressed(buf[:tc.size]); got != tc.value {
			t.Fatalf("ReadCompressed(%#x)=%#x", tc.value, got)
		}
		rest, val, ok := ParseCompressed(buf[:tc.size])
		if !ok || len(rest) != 0 || val != tc.value {
			t.Fatalf("ParseCompressed(%#x) ok=%v rest=%d val=%#x", tc.value, ok, len(rest), val)
		}
		if tc.size > 1 {
			if _, _, ok := ParseCompressed(buf[:tc.size-1]); ok {
				t.Fatalf("ParseCompressed(%#x) expected failure", tc.value)
			}
		}
	}
}

func TestDulintCompressed(t *testing.T) {
	d := makeDulint(0x123456, 0x89abcdef)
	buf := make([]byte, 16)

	size := DulintWriteCompressed(buf, d)
	if size == 0 {
		t.Fatalf("DulintWriteCompressed size=0")
	}
	if got := DulintReadCompressed(buf[:size]); got != d {
		t.Fatalf("DulintReadCompressed=%#v", got)
	}
	if got := DulintGetCompressedSize(d); got != size {
		t.Fatalf("DulintGetCompressedSize=%d", got)
	}
	rest, parsed, ok := DulintParseCompressed(buf[:size])
	if !ok || len(rest) != 0 || parsed != d {
		t.Fatalf("DulintParseCompressed ok=%v rest=%d parsed=%#v", ok, len(rest), parsed)
	}
}

func TestDulintMuchCompressed(t *testing.T) {
	buf := make([]byte, 16)
	dLow := makeDulint(0, 0x7fff)
	size := DulintWriteMuchCompressed(buf, dLow)
	if size == 0 {
		t.Fatalf("DulintWriteMuchCompressed size=0")
	}
	if got := DulintReadMuchCompressed(buf[:size]); got != dLow {
		t.Fatalf("DulintReadMuchCompressed=%#v", got)
	}
	if got := DulintGetMuchCompressedSize(dLow); got != size {
		t.Fatalf("DulintGetMuchCompressedSize=%d", got)
	}

	d := makeDulint(0x1234, 0x56789abc)
	size = DulintWriteMuchCompressed(buf, d)
	if size == 0 {
		t.Fatalf("DulintWriteMuchCompressed size=0")
	}
	if got := DulintReadMuchCompressed(buf[:size]); got != d {
		t.Fatalf("DulintReadMuchCompressed=%#v", got)
	}
	if got := DulintGetMuchCompressedSize(d); got != size {
		t.Fatalf("DulintGetMuchCompressedSize=%d", got)
	}
}

func TestLittleEndianHelpers(t *testing.T) {
	buf := make([]byte, 8)
	WriteTo2LittleEndian(buf[:2], 0x1234)
	if got := ReadFrom2LittleEndian(buf[:2]); got != 0x1234 {
		t.Fatalf("ReadFrom2LittleEndian=%#x", got)
	}

	for size := 1; size <= 8; size++ {
		WriteToNLittleEndian(buf[:size], size, 0x1122334455667788)
		got := ReadFromNLittleEndian(buf[:size], size)
		var mask uint64
		if size == 8 {
			mask = ^uint64(0)
		} else {
			mask = (1 << uint(size*8)) - 1
		}
		want := uint64(0x1122334455667788) & mask
		if got != want {
			t.Fatalf("ReadFromNLittleEndian size=%d got=%#x want=%#x", size, got, want)
		}
	}
}

func TestFloatDoubleEncoding(t *testing.T) {
	var dbl [8]byte
	value := 1234.5
	DoubleWrite(dbl[:], value)
	if got := DoubleRead(dbl[:]); got != value {
		t.Fatalf("DoubleRead=%v", got)
	}
	var dblSrc [8]byte
	ut.NativeEndian.PutUint64(dblSrc[:], math.Float64bits(value))
	var dblDest [8]byte
	DoublePtrWrite(dblDest[:], dblSrc[:])
	var dblExpect [8]byte
	binary.LittleEndian.PutUint64(dblExpect[:], math.Float64bits(value))
	if !bytes.Equal(dblDest[:], dblExpect[:]) {
		t.Fatalf("DoublePtrWrite bytes=%#v", dblDest)
	}

	var flt [4]byte
	fValue := float32(3.25)
	FloatWrite(flt[:], fValue)
	if got := FloatRead(flt[:]); got != fValue {
		t.Fatalf("FloatRead=%v", got)
	}
	var fltSrc [4]byte
	ut.NativeEndian.PutUint32(fltSrc[:], math.Float32bits(fValue))
	var fltDest [4]byte
	FloatPtrWrite(fltDest[:], fltSrc[:])
	var fltExpect [4]byte
	binary.LittleEndian.PutUint32(fltExpect[:], math.Float32bits(fValue))
	if !bytes.Equal(fltDest[:], fltExpect[:]) {
		t.Fatalf("FloatPtrWrite bytes=%#v", fltDest)
	}
}

func TestIntTypeRoundTrip(t *testing.T) {
	unsignedVals := []uint32{0, 1, 0x7fffffff, 0x80000000, 0xffffffff}
	for _, v := range unsignedVals {
		var src [4]byte
		ut.NativeEndian.PutUint32(src[:], v)
		var storage [4]byte
		WriteIntType(storage[:], src[:], true)
		var dst [4]byte
		ReadIntType(dst[:], storage[:], true)
		got := ut.NativeEndian.Uint32(dst[:])
		if got != v {
			t.Fatalf("ReadIntType unsigned %#x got %#x", v, got)
		}
	}

	signedVals := []int32{-2147483648, -1, 0, 1, 2147483647}
	for _, v := range signedVals {
		var src [4]byte
		ut.NativeEndian.PutUint32(src[:], uint32(v))
		var storage [4]byte
		WriteIntType(storage[:], src[:], false)
		var dst [4]byte
		ReadIntType(dst[:], storage[:], false)
		got := int32(ut.NativeEndian.Uint32(dst[:]))
		if got != v {
			t.Fatalf("ReadIntType signed %d got %d", v, got)
		}
	}
}

func TestIntHelpersRoundTrip(t *testing.T) {
	var buf [8]byte
	u64 := uint64(0x0123456789abcdef)
	WriteUint64(buf[:], u64)
	if got := ReadUint64(buf[:]); got != u64 {
		t.Fatalf("ReadUint64=%#x", got)
	}

	i64 := int64(-123456789)
	WriteInt64(buf[:], i64)
	if got := ReadInt64(buf[:]); got != i64 {
		t.Fatalf("ReadInt64=%d", got)
	}

	var buf32 [4]byte
	u32 := uint32(0x89abcdef)
	WriteUint32(buf32[:], u32)
	if got := ReadUint32(buf32[:]); got != u32 {
		t.Fatalf("ReadUint32=%#x", got)
	}

	i32 := int32(-123456)
	WriteInt32(buf32[:], i32)
	if got := ReadInt32(buf32[:]); got != i32 {
		t.Fatalf("ReadInt32=%d", got)
	}
}

func makeDulint(high, low uint32) ut.Dulint {
	return ut.Dulint{High: ut.Ulint(high), Low: ut.Ulint(low)}
}
