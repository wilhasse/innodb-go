package log

import "github.com/wilhasse/innodb-go/mach"

// Keep in sync with mtr/log.go constants.
const (
	mlogSingleRecFlag    = 0x80
	mlog1Byte            = 1
	mlog2Bytes           = 2
	mlog4Bytes           = 4
	mlog8Bytes           = 8
	mlogRecInsert        = 9
	mlogRecUpdateInPlace = 13
	mlogRecDelete        = 14
	mlogWriteStringType  = 30
	mlogMultiRecEnd      = 31
	mlogBiggestType      = 51
)

func mlogParseInitial(buf []byte) ([]byte, byte, uint32, uint32, bool) {
	if len(buf) < 1 {
		return nil, 0, 0, 0, false
	}
	typ := buf[0] &^ mlogSingleRecFlag
	if typ > mlogBiggestType {
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

func mlogParsePayload(typ byte, buf []byte) ([]byte, []byte, bool) {
	var rest []byte
	var ok bool
	switch typ {
	case mlog1Byte, mlog2Bytes, mlog4Bytes, mlog8Bytes:
		rest, ok = mlogParseNBytes(typ, buf, nil)
	case mlogWriteStringType, mlogRecInsert, mlogRecUpdateInPlace:
		rest, ok = mlogParseString(buf, nil)
	case mlogRecDelete:
		rest, ok = mlogParseRecDelete(buf, nil)
	default:
		return nil, nil, false
	}
	if !ok {
		return nil, nil, false
	}
	payloadLen := len(buf) - len(rest)
	if payloadLen < 0 {
		return nil, nil, false
	}
	return buf[:payloadLen], rest, true
}

func mlogApplyRecord(typ byte, payload []byte, page []byte) bool {
	switch typ {
	case mlog1Byte, mlog2Bytes, mlog4Bytes, mlog8Bytes:
		_, ok := mlogParseNBytes(typ, payload, page)
		return ok
	case mlogWriteStringType, mlogRecInsert, mlogRecUpdateInPlace:
		_, ok := mlogParseString(payload, page)
		return ok
	case mlogRecDelete:
		_, ok := mlogParseRecDelete(payload, page)
		return ok
	default:
		return false
	}
}

func mlogParseNBytes(typ byte, buf []byte, page []byte) ([]byte, bool) {
	if len(buf) < 2 {
		return nil, false
	}
	offset := int(mach.ReadFrom2(buf))
	buf = buf[2:]
	if typ == mlog8Bytes {
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
		case mlog1Byte:
			if val > 0xFF || offset+1 > len(page) {
				return nil, false
			}
			mach.WriteTo1(page[offset:], val)
		case mlog2Bytes:
			if val > 0xFFFF || offset+2 > len(page) {
				return nil, false
			}
			mach.WriteTo2(page[offset:], val)
		case mlog4Bytes:
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

func mlogParseString(buf []byte, page []byte) ([]byte, bool) {
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

func mlogParseRecDelete(buf []byte, page []byte) ([]byte, bool) {
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
