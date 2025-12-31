package btr

import "github.com/wilhasse/innodb-go/page"

// LeafInsertBytes appends a record to a leaf page using byte helpers.
func LeafInsertBytes(pageBytes []byte, rec []byte) (uint16, bool) {
	if pageBytes == nil {
		return 0, false
	}
	cur := page.NewSlotCursor(pageBytes)
	_ = cur.Last()
	return page.InsertRecordBytes(pageBytes, rec)
}
