package page

import "github.com/wilhasse/innodb-go/rec"

// DeleteMarkRecord sets the deleted flag and adds the record to the free list.
func DeleteMarkRecord(page []byte, recOff, recLen uint16) bool {
	if page == nil || recOff == 0 {
		return false
	}
	if int(recOff)+1 > len(page) {
		return false
	}
	info := rec.HeaderInfoBits(page[recOff:])
	rec.HeaderSetInfoBits(page[recOff:], info|rec.RecInfoDeletedFlag)
	FreeListPush(page, recOff, recLen)
	nRecs := HeaderGetField(page, PageNRecs)
	if nRecs > 0 {
		HeaderSetField(page, PageNRecs, nRecs-1)
	}
	return true
}
