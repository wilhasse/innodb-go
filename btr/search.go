package btr

import (
	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/rec"
)

// SearchRecordBytes returns the offset of the first record >= key.
func SearchRecordBytes(pageBytes []byte, keyRec []byte, nFields int) (uint16, bool) {
	if pageBytes == nil || nFields <= 0 {
		return 0, false
	}
	keyTuple, err := rec.DecodeVar(keyRec, nFields, 0)
	if err != nil {
		return 0, false
	}
	offsets := collectSlotOffsets(pageBytes)
	entries := collectRecordEntries(pageBytes, offsets, nFields)
	for _, entry := range entries {
		cmp := rec.CompareTuples(entry.tuple, keyTuple, nil, nil)
		if cmp >= 0 {
			return entry.off, cmp == 0
		}
	}
	return 0, false
}

type recordEntry struct {
	off   uint16
	tuple *data.Tuple
}

func collectRecordEntries(pageBytes []byte, offsets []uint16, nFields int) []recordEntry {
	if len(offsets) == 0 {
		return nil
	}
	heapTop := int(page.HeaderGetField(pageBytes, page.PageHeapTop))
	if heapTop == 0 {
		heapTop = int(page.PageDataOffset)
	}
	entries := make([]recordEntry, 0, len(offsets))
	for i, off := range offsets {
		offInt := int(off)
		if offInt < int(page.PageDataOffset) || offInt >= heapTop {
			continue
		}
		next := heapTop
		if i+1 < len(offsets) {
			next = int(offsets[i+1])
		}
		if next <= offInt {
			continue
		}
		recBytes := pageBytes[offInt:next]
		tuple, err := decodeRecordTuple(recBytes, nFields)
		if err != nil {
			continue
		}
		if rec.HeaderInfoBits(recBytes)&rec.RecInfoDeletedFlag != 0 {
			continue
		}
		entries = append(entries, recordEntry{off: off, tuple: tuple})
	}
	return entries
}

func decodeRecordTuple(recBytes []byte, nFields int) (*data.Tuple, error) {
	if nFields <= 0 {
		return nil, nil
	}
	if len(recBytes) >= recordExtra && rec.HeaderInfoBits(recBytes)&rec.RecInfoMinRecFlag != 0 {
		return rec.DecodeVar(recBytes, nFields, recordExtra)
	}
	tuple, err := rec.DecodeVar(recBytes, nFields, 0)
	if err == nil {
		return tuple, nil
	}
	if len(recBytes) >= recordExtra {
		return rec.DecodeVar(recBytes, nFields, recordExtra)
	}
	return nil, err
}
