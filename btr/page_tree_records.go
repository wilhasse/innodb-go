package btr

import (
	"bytes"
	"encoding/binary"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/rec"
)

const recordExtra = rec.RecNNewExtraBytes

func initIndexPageBytes(pageBytes []byte, spaceID uint32, pageNo uint32, level uint16) bool {
	if pageBytes == nil {
		return false
	}
	clear(pageBytes)
	page.PageSetSpaceID(pageBytes, spaceID)
	page.PageSetPageNo(pageBytes, pageNo)
	page.PageSetType(pageBytes, fil.PageTypeIndex)
	page.PageSetPrev(pageBytes, fil.NullPageOffset)
	page.PageSetNext(pageBytes, fil.NullPageOffset)
	page.HeaderSetField(pageBytes, page.PageNDirSlots, 0)
	page.HeaderSetField(pageBytes, page.PageHeapTop, uint16(page.PageDataOffset))
	page.HeaderSetField(pageBytes, page.PageNHeap, 0x8000)
	page.HeaderSetField(pageBytes, page.PageFree, 0)
	page.HeaderSetField(pageBytes, page.PageGarbage, 0)
	page.PageSetLevel(pageBytes, level)

	if !insertSystemRecord(pageBytes, rec.InfimumExtra, rec.InfimumData) {
		return false
	}
	if !insertSystemRecord(pageBytes, rec.SupremumExtra, rec.SupremumData) {
		return false
	}
	page.PageSetNRecs(pageBytes, 0)
	return true
}

func insertSystemRecord(pageBytes []byte, extra []byte, data []byte) bool {
	recBytes := make([]byte, len(extra)+len(data))
	copy(recBytes, extra)
	copy(recBytes[len(extra):], data)
	_, ok := page.InsertRecordBytes(pageBytes, recBytes)
	return ok
}

func rebuildIndexPage(pageBytes []byte, spaceID uint32, pageNo uint32, level uint16, prev, next uint32, records [][]byte) bool {
	if !initIndexPageBytes(pageBytes, spaceID, pageNo, level) {
		return false
	}
	page.PageSetPrev(pageBytes, prev)
	page.PageSetNext(pageBytes, next)
	for _, recBytes := range records {
		if recBytes == nil {
			continue
		}
		if _, ok := page.InsertRecordBytes(pageBytes, recBytes); !ok {
			return false
		}
	}
	page.PageSetNRecs(pageBytes, uint16(len(records)))
	return true
}

func encodeLeafRecord(key, value []byte) []byte {
	tuple := &data.Tuple{Fields: []data.Field{
		{Data: key, Len: uint32(len(key))},
		{Data: value, Len: uint32(len(value))},
	}}
	recBytes, err := rec.EncodeVar(tuple, nil, recordExtra)
	if err != nil {
		return nil
	}
	rec.HeaderSetStatus(recBytes, rec.RecStatusOrdinary)
	rec.HeaderSetInfoBits(recBytes, rec.RecInfoMinRecFlag)
	return recBytes
}

func decodeLeafRecord(recBytes []byte) ([]byte, []byte, bool) {
	tuple, err := rec.DecodeVar(recBytes, 2, recordExtra)
	if err != nil || len(tuple.Fields) < 2 {
		return nil, nil, false
	}
	return tuple.Fields[0].Data, tuple.Fields[1].Data, true
}

func encodeNodePtrRecord(key []byte, childPage uint32) []byte {
	var child [4]byte
	binary.BigEndian.PutUint32(child[:], childPage)
	tuple := &data.Tuple{Fields: []data.Field{
		{Data: key, Len: uint32(len(key))},
		{Data: child[:], Len: 4},
	}}
	recBytes, err := rec.EncodeVar(tuple, nil, recordExtra)
	if err != nil {
		return nil
	}
	rec.HeaderSetStatus(recBytes, rec.RecStatusNodePtr)
	rec.HeaderSetInfoBits(recBytes, rec.RecInfoMinRecFlag)
	return recBytes
}

func decodeNodePtrRecord(recBytes []byte) ([]byte, uint32, bool) {
	tuple, err := rec.DecodeVar(recBytes, 2, recordExtra)
	if err != nil || len(tuple.Fields) < 2 {
		return nil, 0, false
	}
	childBytes := tuple.Fields[1].Data
	if len(childBytes) < 4 {
		return nil, 0, false
	}
	child := binary.BigEndian.Uint32(childBytes[:4])
	return tuple.Fields[0].Data, child, true
}

func recordKey(recBytes []byte) ([]byte, bool) {
	tuple, err := rec.DecodeVar(recBytes, 1, recordExtra)
	if err != nil || len(tuple.Fields) == 0 {
		return nil, false
	}
	return tuple.Fields[0].Data, true
}

func recordKeyOrEmpty(records [][]byte) []byte {
	if len(records) == 0 {
		return nil
	}
	key, ok := recordKey(records[0])
	if !ok {
		return nil
	}
	return key
}

func isSystemRecord(recBytes []byte) bool {
	status := rec.HeaderStatus(recBytes)
	return status == rec.RecStatusInfimum || status == rec.RecStatusSupremum
}

func collectUserRecords(pageBytes []byte) [][]byte {
	offsets := collectSlotOffsets(pageBytes)
	if len(offsets) == 0 {
		return nil
	}
	heapTop := int(page.HeaderGetField(pageBytes, page.PageHeapTop))
	if heapTop == 0 {
		heapTop = int(page.PageDataOffset)
	}
	records := make([][]byte, 0, len(offsets))
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
		recBytes := make([]byte, next-offInt)
		copy(recBytes, pageBytes[offInt:next])
		if rec.HeaderInfoBits(recBytes)&rec.RecInfoDeletedFlag != 0 {
			continue
		}
		if isSystemRecord(recBytes) {
			continue
		}
		records = append(records, recBytes)
	}
	return records
}

func findRecordIndex(records [][]byte, key []byte, compare CompareFunc) (int, bool) {
	for i, recBytes := range records {
		recKey, ok := recordKey(recBytes)
		if !ok {
			continue
		}
		cmp := compare(recKey, key)
		if cmp == 0 {
			return i, true
		}
		if cmp > 0 {
			return i, false
		}
	}
	return len(records), false
}

func insertRecord(records [][]byte, idx int, recBytes []byte) [][]byte {
	if idx < 0 {
		idx = 0
	}
	if idx > len(records) {
		idx = len(records)
	}
	records = append(records, nil)
	copy(records[idx+1:], records[idx:])
	records[idx] = recBytes
	return records
}

func findChildPage(records [][]byte, key []byte, compare CompareFunc) (uint32, bool) {
	var child uint32
	found := false
	for i, recBytes := range records {
		recKey, childPage, ok := decodeNodePtrRecord(recBytes)
		if !ok {
			continue
		}
		if i == 0 && !found {
			child = childPage
			found = true
		}
		cmp := compare(recKey, key)
		if cmp <= 0 {
			child = childPage
			found = true
		} else {
			break
		}
	}
	return child, found
}

func rebuildRecordPage(pageBytes []byte, records [][]byte) bool {
	if pageBytes == nil {
		return false
	}
	clear(pageBytes)
	page.HeaderSetField(pageBytes, page.PageNDirSlots, 0)
	page.HeaderSetField(pageBytes, page.PageHeapTop, uint16(page.PageDataOffset))
	page.HeaderSetField(pageBytes, page.PageNHeap, 0x8000)
	page.HeaderSetField(pageBytes, page.PageFree, 0)
	page.HeaderSetField(pageBytes, page.PageGarbage, 0)
	page.HeaderSetField(pageBytes, page.PageNRecs, 0)
	count := 0
	for _, recBytes := range records {
		if recBytes == nil {
			continue
		}
		if _, ok := page.InsertRecordBytes(pageBytes, recBytes); !ok {
			return false
		}
		count++
	}
	page.PageSetNRecs(pageBytes, uint16(count))
	return true
}

func findRecordOffset(pageBytes []byte, key []byte) uint16 {
	if pageBytes == nil || key == nil {
		return 0
	}
	offsets := collectSlotOffsets(pageBytes)
	if len(offsets) == 0 {
		return 0
	}
	heapTop := int(page.HeaderGetField(pageBytes, page.PageHeapTop))
	if heapTop == 0 {
		heapTop = int(page.PageDataOffset)
	}
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
		if rec.HeaderInfoBits(recBytes)&rec.RecInfoDeletedFlag != 0 {
			continue
		}
		recKey, ok := recordKey(recBytes)
		if !ok {
			continue
		}
		if bytes.Compare(recKey, key) == 0 {
			return off
		}
	}
	return 0
}
