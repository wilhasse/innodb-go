package page

import (
	"bytes"

	"github.com/wilhasse/innodb-go/rem"
)

func isUserRecord(rec Record) bool {
	return rec.Type == rem.RecordUser
}

func compareRecordToKey(rec Record, key []byte) int {
	switch rec.Type {
	case rem.RecordInfimum:
		return -1
	case rem.RecordSupremum:
		return 1
	default:
		return bytes.Compare(rec.Key, key)
	}
}

func nextUserIndex(records []Record, start int) int {
	for i := start; i < len(records); i++ {
		if isUserRecord(records[i]) {
			return i
		}
	}
	return len(records)
}

func prevUserIndex(records []Record, start int) int {
	for i := start; i >= 0; i-- {
		if isUserRecord(records[i]) {
			return i
		}
	}
	return -1
}
