package mtr

import (
	"github.com/wilhasse/innodb-go/dyn"
	"github.com/wilhasse/innodb-go/mach"
	"github.com/wilhasse/innodb-go/ut"
)

const (
	MlogSingleRecFlag = 0x80

	Mlog1Byte  = 1
	Mlog2Bytes = 2
	Mlog4Bytes = 4
	Mlog8Bytes = 8

	MlogRecInsert        = 9
	MlogRecUpdateInPlace = 13
	MlogRecDelete        = 14

	MlogWriteStringType = 30
	MlogMultiRecEnd     = 31
	MlogBiggestType     = 51
)

const (
	filPageOffset             = 4
	filPageArchLogNoOrSpaceID = 34
)

// MlogOpen reserves a buffer in the mini-transaction log.
func MlogOpen(mtr *Mtr, size int) []byte {
	if mtr == nil {
		return nil
	}
	mtr.Modifications = true
	if mtr.LogMode == LogNone {
		return nil
	}
	if mtr.Log == nil {
		mtr.Log = dyn.New()
	}
	return mtr.Log.Open(size)
}

// MlogClose closes a buffer opened with MlogOpen using the bytes consumed.
func MlogClose(mtr *Mtr, used int) {
	if mtr == nil || mtr.LogMode == LogNone || mtr.Log == nil {
		return
	}
	mtr.Log.Close(used)
}

// MlogCatenateUlint appends a fixed-size integer to the log.
func MlogCatenateUlint(mtr *Mtr, val uint32, typ byte) {
	if mtr == nil || mtr.LogMode == LogNone {
		return
	}
	if mtr.Log == nil {
		mtr.Log = dyn.New()
	}
	buf := mtr.Log.Push(int(typ))
	if buf == nil {
		return
	}
	switch typ {
	case Mlog4Bytes:
		mach.WriteTo4(buf, val)
	case Mlog2Bytes:
		mach.WriteTo2(buf, val)
	case Mlog1Byte:
		mach.WriteTo1(buf, val)
	}
}

// MlogCatenateString appends a byte slice to the log.
func MlogCatenateString(mtr *Mtr, data []byte) {
	if mtr == nil || mtr.LogMode == LogNone || len(data) == 0 {
		return
	}
	if mtr.Log == nil {
		mtr.Log = dyn.New()
	}
	mtr.Log.PushBytes(data)
}

// MlogCatenateUlintCompressed appends a compressed ulint to the log.
func MlogCatenateUlintCompressed(mtr *Mtr, val uint32) {
	buf := MlogOpen(mtr, 10)
	if buf == nil {
		return
	}
	used := mach.WriteCompressed(buf, val)
	MlogClose(mtr, used)
}

// MlogCatenateDulintCompressed appends a compressed dulint to the log.
func MlogCatenateDulintCompressed(mtr *Mtr, val ut.Dulint) {
	buf := MlogOpen(mtr, 15)
	if buf == nil {
		return
	}
	used := mach.DulintWriteCompressed(buf, val)
	MlogClose(mtr, used)
}

// MlogWriteInitialLogRecord writes the type and page identifiers.
func MlogWriteInitialLogRecord(page []byte, typ byte, mtr *Mtr) {
	logPtr := MlogOpen(mtr, 11)
	if logPtr == nil {
		return
	}
	used := MlogWriteInitialLogRecordFast(page, typ, logPtr, mtr)
	MlogClose(mtr, used)
}

// MlogWriteInitialLogRecordFast writes the initial record into logPtr.
func MlogWriteInitialLogRecordFast(page []byte, typ byte, logPtr []byte, mtr *Mtr) int {
	if len(logPtr) < 3 || len(page) < filPageArchLogNoOrSpaceID+4 {
		return 0
	}
	if typ > MlogBiggestType {
		return 0
	}
	space := mach.ReadFrom4(page[filPageArchLogNoOrSpaceID:])
	pageNo := mach.ReadFrom4(page[filPageOffset:])
	logPtr[0] = typ
	pos := 1
	pos += mach.WriteCompressed(logPtr[pos:], space)
	pos += mach.WriteCompressed(logPtr[pos:], pageNo)
	if mtr != nil {
		mtr.NLogRecs++
	}
	return pos
}

// MlogWriteUlint writes a 1-4 byte integer and logs it.
func MlogWriteUlint(page []byte, offset int, val uint32, typ byte, mtr *Mtr) {
	if offset < 0 || offset >= len(page) {
		return
	}
	switch typ {
	case Mlog1Byte:
		if offset+1 > len(page) {
			return
		}
		mach.WriteTo1(page[offset:], val)
	case Mlog2Bytes:
		if offset+2 > len(page) {
			return
		}
		mach.WriteTo2(page[offset:], val)
	case Mlog4Bytes:
		if offset+4 > len(page) {
			return
		}
		mach.WriteTo4(page[offset:], val)
	default:
		return
	}

	logPtr := MlogOpen(mtr, 18)
	if logPtr == nil {
		return
	}
	pos := MlogWriteInitialLogRecordFast(page, typ, logPtr, mtr)
	if pos == 0 {
		MlogClose(mtr, 0)
		return
	}
	if pos+2 > len(logPtr) {
		MlogClose(mtr, pos)
		return
	}
	mach.WriteTo2(logPtr[pos:], uint32(offset))
	pos += 2
	pos += mach.WriteCompressed(logPtr[pos:], val)
	MlogClose(mtr, pos)
}

// MlogWriteDulint writes an 8-byte integer and logs it.
func MlogWriteDulint(page []byte, offset int, val ut.Dulint, mtr *Mtr) {
	if offset < 0 || offset+8 > len(page) {
		return
	}
	mach.WriteTo8(page[offset:], val)

	logPtr := MlogOpen(mtr, 22)
	if logPtr == nil {
		return
	}
	pos := MlogWriteInitialLogRecordFast(page, Mlog8Bytes, logPtr, mtr)
	if pos == 0 {
		MlogClose(mtr, 0)
		return
	}
	mach.WriteTo2(logPtr[pos:], uint32(offset))
	pos += 2
	pos += mach.DulintWriteCompressed(logPtr[pos:], val)
	MlogClose(mtr, pos)
}

// MlogWriteString writes data to the page and logs the operation.
func MlogWriteString(page []byte, offset int, data []byte, mtr *Mtr) {
	if offset < 0 || offset+len(data) > len(page) {
		return
	}
	copy(page[offset:], data)
	MlogLogString(page, offset, len(data), mtr)
}

// MlogLogString logs a write of a string to a page.
func MlogLogString(page []byte, offset int, length int, mtr *Mtr) {
	if length < 0 || offset < 0 {
		return
	}
	logPtr := MlogOpen(mtr, 30)
	if logPtr == nil {
		return
	}
	pos := MlogWriteInitialLogRecordFast(page, MlogWriteStringType, logPtr, mtr)
	if pos == 0 {
		MlogClose(mtr, 0)
		return
	}
	if pos+4 > len(logPtr) {
		MlogClose(mtr, pos)
		return
	}
	mach.WriteTo2(logPtr[pos:], uint32(offset))
	pos += 2
	mach.WriteTo2(logPtr[pos:], uint32(length))
	pos += 2
	MlogClose(mtr, pos)
	if length > 0 {
		MlogCatenateString(mtr, page[offset:offset+length])
	}
}

// MlogWriteRecInsert writes a record payload and logs the insert.
func MlogWriteRecInsert(page []byte, offset int, data []byte, mtr *Mtr) {
	if offset < 0 || offset+len(data) > len(page) {
		return
	}
	copy(page[offset:], data)
	mlogLogRecordChange(page, offset, data, MlogRecInsert, mtr)
}

// MlogWriteRecUpdateInPlace writes record payload updates and logs them.
func MlogWriteRecUpdateInPlace(page []byte, offset int, data []byte, mtr *Mtr) {
	if offset < 0 || offset+len(data) > len(page) {
		return
	}
	copy(page[offset:], data)
	mlogLogRecordChange(page, offset, data, MlogRecUpdateInPlace, mtr)
}

// MlogWriteRecDelete clears a record payload and logs the delete.
func MlogWriteRecDelete(page []byte, offset int, length int, mtr *Mtr) {
	if offset < 0 || length < 0 || offset+length > len(page) {
		return
	}
	if length > 0 {
		clear(page[offset : offset+length])
	}
	logPtr := MlogOpen(mtr, 18)
	if logPtr == nil {
		return
	}
	pos := MlogWriteInitialLogRecordFast(page, MlogRecDelete, logPtr, mtr)
	if pos == 0 {
		MlogClose(mtr, 0)
		return
	}
	if pos+4 > len(logPtr) {
		MlogClose(mtr, pos)
		return
	}
	mach.WriteTo2(logPtr[pos:], uint32(offset))
	pos += 2
	mach.WriteTo2(logPtr[pos:], uint32(length))
	pos += 2
	MlogClose(mtr, pos)
}

func mlogLogRecordChange(page []byte, offset int, data []byte, typ byte, mtr *Mtr) {
	if len(data) == 0 {
		return
	}
	logPtr := MlogOpen(mtr, 30)
	if logPtr == nil {
		return
	}
	pos := MlogWriteInitialLogRecordFast(page, typ, logPtr, mtr)
	if pos == 0 {
		MlogClose(mtr, 0)
		return
	}
	if pos+4 > len(logPtr) {
		MlogClose(mtr, pos)
		return
	}
	mach.WriteTo2(logPtr[pos:], uint32(offset))
	pos += 2
	mach.WriteTo2(logPtr[pos:], uint32(len(data)))
	pos += 2
	MlogClose(mtr, pos)
	MlogCatenateString(mtr, data)
}

// MlogParseInitialLogRecord parses type/space/page from a log buffer.
func MlogParseInitialLogRecord(buf []byte) ([]byte, byte, uint32, uint32, bool) {
	if len(buf) < 1 {
		return nil, 0, 0, 0, false
	}
	typ := buf[0] &^ MlogSingleRecFlag
	if typ > MlogBiggestType {
		return nil, 0, 0, 0, false
	}
	rest, space, ok := mach.ParseCompressed(buf[1:])
	if !ok {
		return nil, 0, 0, 0, false
	}
	rest, pageNo, ok := mach.ParseCompressed(rest)
	if !ok {
		return nil, 0, 0, 0, false
	}
	return rest, typ, space, pageNo, true
}

// MlogParseNBytes parses a numeric log record and applies it to page.
func MlogParseNBytes(typ byte, buf []byte, page []byte) ([]byte, bool) {
	if len(buf) < 2 {
		return nil, false
	}
	offset := int(mach.ReadFrom2(buf))
	buf = buf[2:]
	if typ == Mlog8Bytes {
		rest, dval, ok := mach.DulintParseCompressed(buf)
		if !ok {
			return nil, false
		}
		if page != nil && offset+8 <= len(page) {
			mach.WriteTo8(page[offset:], dval)
		}
		return rest, true
	}

	rest, val, ok := mach.ParseCompressed(buf)
	if !ok {
		return nil, false
	}
	if page != nil && offset >= 0 {
		switch typ {
		case Mlog1Byte:
			if val > 0xFF || offset+1 > len(page) {
				return nil, false
			}
			mach.WriteTo1(page[offset:], val)
		case Mlog2Bytes:
			if val > 0xFFFF || offset+2 > len(page) {
				return nil, false
			}
			mach.WriteTo2(page[offset:], val)
		case Mlog4Bytes:
			if offset+4 > len(page) {
				return nil, false
			}
			mach.WriteTo4(page[offset:], val)
		default:
			return nil, false
		}
	}
	return rest, true
}

// MlogParseString parses a string log record and applies it to page.
func MlogParseString(buf []byte, page []byte) ([]byte, bool) {
	if len(buf) < 4 {
		return nil, false
	}
	offset := int(mach.ReadFrom2(buf))
	length := int(mach.ReadFrom2(buf[2:]))
	buf = buf[4:]
	if length < 0 || offset < 0 {
		return nil, false
	}
	if len(buf) < length {
		return nil, false
	}
	if page != nil && offset+length <= len(page) {
		copy(page[offset:], buf[:length])
	}
	return buf[length:], true
}

// MlogParseRecInsert parses a record insert and applies it to page.
func MlogParseRecInsert(buf []byte, page []byte) ([]byte, bool) {
	return MlogParseString(buf, page)
}

// MlogParseRecUpdateInPlace parses an in-place record update.
func MlogParseRecUpdateInPlace(buf []byte, page []byte) ([]byte, bool) {
	return MlogParseString(buf, page)
}

// MlogParseRecDelete parses a record delete and applies it to page.
func MlogParseRecDelete(buf []byte, page []byte) ([]byte, bool) {
	if len(buf) < 4 {
		return nil, false
	}
	offset := int(mach.ReadFrom2(buf))
	length := int(mach.ReadFrom2(buf[2:]))
	buf = buf[4:]
	if length < 0 || offset < 0 {
		return nil, false
	}
	if page != nil && offset+length <= len(page) {
		clear(page[offset : offset+length])
	}
	return buf, true
}
