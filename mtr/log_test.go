package mtr

import (
	"bytes"
	"testing"

	"github.com/wilhasse/innodb-go/mach"
	"github.com/wilhasse/innodb-go/ut"
)

func TestMlogInitialRecordParse(t *testing.T) {
	m := New()
	page := makePage(7, 42)

	MlogWriteInitialLogRecord(page, MlogWriteStringType, m)
	data := m.LogBytes()
	rest, typ, space, pageNo, ok := MlogParseInitialLogRecord(data)
	if !ok {
		t.Fatalf("parse failed")
	}
	if typ != MlogWriteStringType {
		t.Fatalf("type=%d", typ)
	}
	if space != 7 || pageNo != 42 {
		t.Fatalf("space/page=%d/%d", space, pageNo)
	}
	if len(rest) != 0 {
		t.Fatalf("expected empty rest, got %d bytes", len(rest))
	}
}

func TestMlogWriteUlintAndParse(t *testing.T) {
	m := New()
	page := makePage(3, 9)
	offset := 100

	MlogWriteUlint(page, offset, 0x1234, Mlog2Bytes, m)
	if got := mach.ReadFrom2(page[offset:]); got != 0x1234 {
		t.Fatalf("page write=%#x", got)
	}

	logData := m.LogBytes()
	rest, typ, space, pageNo, ok := MlogParseInitialLogRecord(logData)
	if !ok || typ != Mlog2Bytes || space != 3 || pageNo != 9 {
		t.Fatalf("initial parse ok=%v typ=%d space=%d page=%d", ok, typ, space, pageNo)
	}
	page2 := make([]byte, ut.UNIV_PAGE_SIZE)
	rest, ok = MlogParseNBytes(typ, rest, page2)
	if !ok || len(rest) != 0 {
		t.Fatalf("parse nbytes ok=%v rest=%d", ok, len(rest))
	}
	if got := mach.ReadFrom2(page2[offset:]); got != 0x1234 {
		t.Fatalf("parsed value=%#x", got)
	}
}

func TestMlogWriteDulintAndParse(t *testing.T) {
	m := New()
	page := makePage(1, 2)
	offset := 200
	val := ut.Dulint{High: ut.Ulint(0x11223344), Low: ut.Ulint(0x55667788)}

	MlogWriteDulint(page, offset, val, m)
	if got := mach.ReadFrom8(page[offset:]); got != val {
		t.Fatalf("page write=%#v", got)
	}

	logData := m.LogBytes()
	rest, typ, space, pageNo, ok := MlogParseInitialLogRecord(logData)
	if !ok || typ != Mlog8Bytes || space != 1 || pageNo != 2 {
		t.Fatalf("initial parse ok=%v typ=%d space=%d page=%d", ok, typ, space, pageNo)
	}
	page2 := make([]byte, ut.UNIV_PAGE_SIZE)
	rest, ok = MlogParseNBytes(typ, rest, page2)
	if !ok || len(rest) != 0 {
		t.Fatalf("parse nbytes ok=%v rest=%d", ok, len(rest))
	}
	if got := mach.ReadFrom8(page2[offset:]); got != val {
		t.Fatalf("parsed value=%#v", got)
	}
}

func TestMlogWriteStringAndParse(t *testing.T) {
	m := New()
	page := makePage(4, 12)
	offset := 300
	data := []byte("hello")

	MlogWriteString(page, offset, data, m)
	if !bytes.Equal(page[offset:offset+len(data)], data) {
		t.Fatalf("page write mismatch")
	}

	logData := m.LogBytes()
	rest, typ, space, pageNo, ok := MlogParseInitialLogRecord(logData)
	if !ok || typ != MlogWriteStringType || space != 4 || pageNo != 12 {
		t.Fatalf("initial parse ok=%v typ=%d space=%d page=%d", ok, typ, space, pageNo)
	}
	page2 := make([]byte, ut.UNIV_PAGE_SIZE)
	rest, ok = MlogParseString(rest, page2)
	if !ok || len(rest) != 0 {
		t.Fatalf("parse string ok=%v rest=%d", ok, len(rest))
	}
	if !bytes.Equal(page2[offset:offset+len(data)], data) {
		t.Fatalf("parsed string mismatch")
	}
}

func TestMlogRecInsertAndParse(t *testing.T) {
	m := New()
	page := makePage(5, 7)
	offset := 64
	data := []byte{0xAA, 0xBB, 0xCC}

	MlogWriteRecInsert(page, offset, data, m)
	if !bytes.Equal(page[offset:offset+len(data)], data) {
		t.Fatalf("insert did not write data")
	}

	logData := m.LogBytes()
	rest, typ, space, pageNo, ok := MlogParseInitialLogRecord(logData)
	if !ok || typ != MlogRecInsert || space != 5 || pageNo != 7 {
		t.Fatalf("initial parse ok=%v typ=%d space=%d page=%d", ok, typ, space, pageNo)
	}
	page2 := make([]byte, ut.UNIV_PAGE_SIZE)
	rest, ok = MlogParseRecInsert(rest, page2)
	if !ok || len(rest) != 0 {
		t.Fatalf("parse insert ok=%v rest=%d", ok, len(rest))
	}
	if !bytes.Equal(page2[offset:offset+len(data)], data) {
		t.Fatalf("parsed insert mismatch")
	}
}

func TestMlogRecUpdateAndParse(t *testing.T) {
	m := New()
	page := makePage(6, 8)
	offset := 80
	data := []byte{0x10, 0x20}

	MlogWriteRecUpdateInPlace(page, offset, data, m)
	if !bytes.Equal(page[offset:offset+len(data)], data) {
		t.Fatalf("update did not write data")
	}

	logData := m.LogBytes()
	rest, typ, space, pageNo, ok := MlogParseInitialLogRecord(logData)
	if !ok || typ != MlogRecUpdateInPlace || space != 6 || pageNo != 8 {
		t.Fatalf("initial parse ok=%v typ=%d space=%d page=%d", ok, typ, space, pageNo)
	}
	page2 := make([]byte, ut.UNIV_PAGE_SIZE)
	rest, ok = MlogParseRecUpdateInPlace(rest, page2)
	if !ok || len(rest) != 0 {
		t.Fatalf("parse update ok=%v rest=%d", ok, len(rest))
	}
	if !bytes.Equal(page2[offset:offset+len(data)], data) {
		t.Fatalf("parsed update mismatch")
	}
}

func TestMlogRecDeleteAndParse(t *testing.T) {
	m := New()
	page := makePage(9, 10)
	offset := 96
	length := 4
	copy(page[offset:offset+length], []byte{1, 2, 3, 4})

	MlogWriteRecDelete(page, offset, length, m)
	if !bytes.Equal(page[offset:offset+length], make([]byte, length)) {
		t.Fatalf("delete did not clear data")
	}

	logData := m.LogBytes()
	rest, typ, space, pageNo, ok := MlogParseInitialLogRecord(logData)
	if !ok || typ != MlogRecDelete || space != 9 || pageNo != 10 {
		t.Fatalf("initial parse ok=%v typ=%d space=%d page=%d", ok, typ, space, pageNo)
	}
	page2 := make([]byte, ut.UNIV_PAGE_SIZE)
	copy(page2[offset:offset+length], []byte{9, 9, 9, 9})
	rest, ok = MlogParseRecDelete(rest, page2)
	if !ok || len(rest) != 0 {
		t.Fatalf("parse delete ok=%v rest=%d", ok, len(rest))
	}
	if !bytes.Equal(page2[offset:offset+length], make([]byte, length)) {
		t.Fatalf("parsed delete mismatch")
	}
}

func makePage(space, pageNo uint32) []byte {
	page := make([]byte, ut.UNIV_PAGE_SIZE)
	mach.WriteTo4(page[filPageArchLogNoOrSpaceID:], space)
	mach.WriteTo4(page[filPageOffset:], pageNo)
	return page
}
