package btr

import (
	"bytes"

	"github.com/wilhasse/innodb-go/page"
)

// LeafInsertBytes inserts a record into a leaf page and maintains order.
func LeafInsertBytes(pageBytes []byte, recBytes []byte) (uint16, bool) {
	if pageBytes == nil || len(recBytes) == 0 {
		return 0, false
	}
	key, ok := recordKey(recBytes)
	if !ok {
		return page.InsertRecordBytes(pageBytes, recBytes)
	}
	records := collectUserRecords(pageBytes)
	idx, _ := findRecordIndex(records, key, bytes.Compare)
	records = insertRecord(records, idx, recBytes)
	if !rebuildRecordPage(pageBytes, records) {
		return 0, false
	}
	return findRecordOffset(pageBytes, key), true
}
