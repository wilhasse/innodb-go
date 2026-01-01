package api

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"

	"github.com/wilhasse/innodb-go/btr"
	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/lock"
	"github.com/wilhasse/innodb-go/read"
	"github.com/wilhasse/innodb-go/rec"
	"github.com/wilhasse/innodb-go/row"
	"github.com/wilhasse/innodb-go/trx"
)

// LockMode mirrors cursor lock modes.
type LockMode int

const (
	LockIX LockMode = iota + 1
	LockIS
)

// CursorMode controls cursor movement.
type CursorMode int

const (
	CursorGE CursorMode = iota
	CursorG
)

// MatchMode mirrors ib_match_t.
type MatchMode int

const (
	IB_EXACT_MATCH MatchMode = iota
	IB_CLOSEST_MATCH
	IB_EXACT_PREFIX
)

// Cursor provides simple table iteration.
type Cursor struct {
	Table      *Table
	Tree       *btr.Tree
	Index      *row.SecondaryIndex
	treeCur    *btr.Cursor
	pageCur    *btr.PageCursor
	pcur       *btr.Pcur
	lastKey    []byte
	virtualRow *data.Tuple
	Trx        *trx.Trx
	MatchMode  MatchMode
	LockMode   LockMode
}

func (crsr *Cursor) usePageTree() bool {
	return crsr != nil && crsr.Index == nil && crsr.Table != nil && crsr.Table.Store != nil && crsr.Table.Store.PageTree != nil
}

// CursorOpenTable opens a cursor on a table.
func CursorOpenTable(name string, ibTrx *trx.Trx, out **Cursor) ErrCode {
	if out == nil {
		return DB_ERROR
	}
	table := findTable(name)
	if table == nil {
		return DB_TABLE_NOT_FOUND
	}
	var tree *btr.Tree
	if table.Store != nil {
		tree = table.Store.Tree
	}
	cursor := &Cursor{Table: table, Tree: tree, Trx: ibTrx, MatchMode: IB_CLOSEST_MATCH, LockMode: LockIS}
	if tree != nil {
		cursor.pcur = btr.NewPcur(tree)
	}
	if ibTrx != nil {
		trx.TrxAssignReadView(ibTrx)
	}
	*out = cursor
	return DB_SUCCESS
}

// CursorClose closes a cursor.
func CursorClose(crsr *Cursor) ErrCode {
	if crsr != nil && crsr.pcur != nil {
		crsr.pcur.Free()
	}
	if crsr != nil {
		crsr.pageCur = nil
	}
	return DB_SUCCESS
}

// CursorLock is a no-op for the in-memory cursor.
func CursorLock(crsr *Cursor, mode LockMode) ErrCode {
	return CursorSetLockMode(crsr, mode)
}

// CursorAttachTrx binds a transaction to a cursor.
func CursorAttachTrx(crsr *Cursor, ibTrx *trx.Trx) ErrCode {
	if crsr == nil {
		return DB_ERROR
	}
	crsr.Trx = ibTrx
	if ibTrx != nil {
		trx.TrxAssignReadView(ibTrx)
	}
	return DB_SUCCESS
}

// CursorReset resets cursor position.
func CursorReset(crsr *Cursor) ErrCode {
	if crsr == nil {
		return DB_ERROR
	}
	if crsr.pcur != nil {
		crsr.pcur.Init()
	}
	crsr.treeCur = nil
	crsr.pageCur = nil
	crsr.lastKey = nil
	crsr.virtualRow = nil
	return DB_SUCCESS
}

// CursorInsertRow inserts a tuple via the cursor.
func CursorInsertRow(crsr *Cursor, tpl *data.Tuple) ErrCode {
	if crsr == nil || crsr.Table == nil || crsr.Table.Store == nil {
		return DB_ERROR
	}
	if err := validateNotNull(crsr, tpl); err != DB_SUCCESS {
		return err
	}
	encoded, err := encodeDecodeTuple(tpl)
	if err != DB_SUCCESS {
		return err
	}
	if err := lockTableForDML(crsr); err != DB_SUCCESS {
		return err
	}
	if err := lockRecordForDML(crsr, encoded, lock.ModeX, lock.FlagInsertIntention); err != DB_SUCCESS {
		return err
	}
	if err := crsr.Table.Store.Insert(encoded); err != nil {
		if errors.Is(err, row.ErrDuplicateKey) {
			return DB_DUPLICATE_KEY
		}
		return DB_ERROR
	}
	recordUndoInsert(crsr, encoded)
	recordRowVersionForKey(crsr, nil, encoded)
	return DB_SUCCESS
}

func validateNotNull(crsr *Cursor, tpl *data.Tuple) ErrCode {
	if crsr == nil || crsr.Table == nil || crsr.Table.Schema == nil || tpl == nil {
		return DB_ERROR
	}
	for i, col := range crsr.Table.Schema.Columns {
		if col.Attr&IB_COL_NOT_NULL == 0 {
			continue
		}
		if i >= len(tpl.Fields) {
			return DB_DATA_MISMATCH
		}
		field := tpl.Fields[i]
		if field.Len == data.UnivSQLNull || (field.Len == 0 && len(field.Data) == 0) {
			return DB_DATA_MISMATCH
		}
	}
	return DB_SUCCESS
}

// CursorFirst positions the cursor at the first row.
func CursorFirst(crsr *Cursor) ErrCode {
	if crsr == nil || crsr.Table == nil {
		return DB_ERROR
	}
	if crsr.Index != nil && crsr.Table.Store != nil {
		if err := crsr.Table.Store.MergeSecondaryIndexBuffer(crsr.Index); err != nil {
			return DB_ERROR
		}
	}
	if crsr.usePageTree() {
		cur, err := crsr.Table.Store.PageTree.First()
		if err != nil {
			return DB_ERROR
		}
		crsr.pageCur = cur
		crsr.treeCur = nil
		crsr.virtualRow = nil
		if crsr.pageCur == nil || !crsr.pageCur.Valid() {
			return DB_RECORD_NOT_FOUND
		}
		if !advancePageCursorVisible(crsr) {
			return DB_RECORD_NOT_FOUND
		}
		crsr.lastKey = crsr.pageCur.Key()
		return DB_SUCCESS
	}
	if crsr.Tree == nil {
		return DB_ERROR
	}
	pcur := ensurePcur(crsr)
	if pcur == nil {
		return DB_ERROR
	}
	if !pcur.OpenAtIndexSide(true) {
		return DB_RECORD_NOT_FOUND
	}
	crsr.treeCur = pcur.Cur.Cursor
	if crsr.treeCur == nil || !crsr.treeCur.Valid() {
		return DB_RECORD_NOT_FOUND
	}
	if !advanceTreeCursorVisible(crsr) {
		return DB_RECORD_NOT_FOUND
	}
	return DB_SUCCESS
}

// CursorNext advances the cursor.
func CursorNext(crsr *Cursor) ErrCode {
	if crsr == nil || crsr.Table == nil {
		return DB_ERROR
	}
	if crsr.usePageTree() {
		crsr.treeCur = nil
		if crsr.pageCur != nil && crsr.pageCur.Valid() {
			crsr.lastKey = crsr.pageCur.Key()
			if !crsr.pageCur.Next() {
				crsr.pageCur = nil
				return DB_END_OF_INDEX
			}
		} else {
			if len(crsr.lastKey) == 0 {
				return DB_END_OF_INDEX
			}
			cur, _, err := crsr.Table.Store.PageTree.Seek(crsr.lastKey, btr.SearchGE)
			if err != nil || cur == nil || !cur.Valid() {
				return DB_END_OF_INDEX
			}
			if bytes.Equal(cur.Key(), crsr.lastKey) {
				if !cur.Next() {
					return DB_END_OF_INDEX
				}
			}
			crsr.pageCur = cur
		}
		if !advancePageCursorVisible(crsr) {
			return DB_END_OF_INDEX
		}
		crsr.lastKey = crsr.pageCur.Key()
		return DB_SUCCESS
	}
	if crsr.Tree == nil {
		return DB_ERROR
	}
	pcur := ensurePcur(crsr)
	if pcur == nil || pcur.Cur == nil {
		return DB_ERROR
	}
	if pcur.Cur.Valid() {
		crsr.lastKey = pcur.Cur.Key()
		if !pcur.Cur.Next() {
			crsr.treeCur = nil
			return DB_END_OF_INDEX
		}
		if !advanceTreeCursorVisible(crsr) {
			return DB_END_OF_INDEX
		}
		return DB_SUCCESS
	}
	if len(crsr.lastKey) == 0 {
		return DB_END_OF_INDEX
	}
	if !pcur.OpenOnUserRec(crsr.lastKey, btr.SearchGE) {
		return DB_END_OF_INDEX
	}
	if pcur.Cur.Valid() && row.CompareKeys(pcur.Cur.Key(), crsr.lastKey) == 0 {
		if !pcur.Cur.Next() {
			crsr.treeCur = nil
			return DB_END_OF_INDEX
		}
	}
	if !pcur.Cur.Valid() {
		return DB_END_OF_INDEX
	}
	crsr.treeCur = pcur.Cur.Cursor
	if !advanceTreeCursorVisible(crsr) {
		return DB_END_OF_INDEX
	}
	return DB_SUCCESS
}

// CursorReadRow reads the current row into tpl.
func CursorReadRow(crsr *Cursor, tpl *data.Tuple) ErrCode {
	if crsr == nil || crsr.Table == nil || tpl == nil {
		return DB_ERROR
	}
	if crsr.virtualRow != nil {
		copyTuple(tpl, crsr.virtualRow)
		return DB_SUCCESS
	}
	if crsr.usePageTree() && crsr.pageCur != nil {
		rowTuple, ok := cursorPageVisibleTuple(crsr)
		if !ok {
			return DB_RECORD_NOT_FOUND
		}
		copyTuple(tpl, rowTuple)
		return DB_SUCCESS
	}
	if crsr.Tree == nil {
		return DB_ERROR
	}
	if crsr.treeCur == nil || !crsr.treeCur.Valid() {
		return DB_RECORD_NOT_FOUND
	}
	value := crsr.treeCur.Value()
	if len(value) == 0 {
		return DB_RECORD_NOT_FOUND
	}
	recBytes := value
	if len(value) >= 8 {
		recBytes = value[8:]
	}
	if len(recBytes) == 0 {
		rowID, rowTuple, ok := cursorRow(crsr)
		if !ok || rowTuple == nil {
			return DB_RECORD_NOT_FOUND
		}
		recordKey := []byte(nil)
		if crsr.Index == nil {
			recordKey = crsr.treeCur.Key()
		}
		visible, ok := cursorVisibleTuple(crsr, rowID, rowTuple, recordKey)
		if !ok {
			return DB_RECORD_NOT_FOUND
		}
		copyTuple(tpl, visible)
		return DB_SUCCESS
	}
	nFields := len(tpl.Fields)
	if nFields == 0 && crsr.Table != nil && crsr.Table.Schema != nil {
		nFields = len(crsr.Table.Schema.Columns)
	}
	decoded, err := rec.DecodeVar(recBytes, nFields, 0)
	if err != nil {
		rowID, rowTuple, ok := cursorRow(crsr)
		if !ok || rowTuple == nil {
			return DB_ERROR
		}
		recordKey := []byte(nil)
		if crsr.Index == nil {
			recordKey = crsr.treeCur.Key()
		}
		visible, ok := cursorVisibleTuple(crsr, rowID, rowTuple, recordKey)
		if !ok {
			return DB_RECORD_NOT_FOUND
		}
		copyTuple(tpl, visible)
		return DB_SUCCESS
	}
	rowID, _ := row.DecodeRowID(value)
	recordKey := []byte(nil)
	if crsr.Index == nil {
		recordKey = crsr.treeCur.Key()
	}
	visible, ok := cursorVisibleTuple(crsr, rowID, decoded, recordKey)
	if !ok {
		return DB_RECORD_NOT_FOUND
	}
	copyTuple(tpl, visible)
	return DB_SUCCESS
}

func cursorRow(crsr *Cursor) (uint64, *data.Tuple, bool) {
	if crsr == nil || crsr.Table == nil || crsr.Table.Store == nil {
		return 0, nil, false
	}
	if crsr.usePageTree() && crsr.pageCur != nil {
		if crsr.pageCur == nil || !crsr.pageCur.Valid() {
			return 0, nil, false
		}
		value := crsr.pageCur.Value()
		rowID, ok := row.DecodeRowID(value)
		if !ok {
			return 0, nil, false
		}
		rowTuple := crsr.Table.Store.RowByID(rowID)
		if rowTuple != nil {
			return rowID, rowTuple, true
		}
		decoded, ok := decodeCursorTuple(crsr, value)
		return rowID, decoded, ok
	}
	if crsr.treeCur == nil && crsr.pcur != nil && crsr.pcur.Cur != nil && crsr.pcur.Cur.Valid() {
		crsr.treeCur = crsr.pcur.Cur.Cursor
	}
	if crsr.treeCur == nil || !crsr.treeCur.Valid() {
		return 0, nil, false
	}
	rowID, ok := row.DecodeRowID(crsr.treeCur.Value())
	if !ok {
		return 0, nil, false
	}
	rowTuple := crsr.Table.Store.RowByID(rowID)
	if rowTuple == nil {
		return rowID, nil, false
	}
	return rowID, rowTuple, true
}

func cursorPageVisibleTuple(crsr *Cursor) (*data.Tuple, bool) {
	rowID, tuple, ok := cursorPageTuple(crsr)
	if !ok || tuple == nil {
		return nil, false
	}
	recordKey := []byte(nil)
	if crsr != nil && crsr.Index == nil && crsr.pageCur != nil {
		recordKey = crsr.pageCur.Key()
	}
	return cursorVisibleTuple(crsr, rowID, tuple, recordKey)
}

func cursorPageTuple(crsr *Cursor) (uint64, *data.Tuple, bool) {
	if crsr == nil || crsr.Table == nil || crsr.Table.Store == nil {
		return 0, nil, false
	}
	if crsr.pageCur == nil || !crsr.pageCur.Valid() {
		return 0, nil, false
	}
	rowID, tuple, ok := decodeCursorValue(crsr, crsr.pageCur.Value())
	if !ok || tuple == nil {
		return 0, nil, false
	}
	return rowID, tuple, true
}

func advancePageCursorVisible(crsr *Cursor) bool {
	if crsr == nil {
		return false
	}
	for crsr.pageCur != nil && crsr.pageCur.Valid() {
		if _, ok := cursorPageVisibleTuple(crsr); ok {
			return true
		}
		if !crsr.pageCur.Next() {
			crsr.pageCur = nil
			return false
		}
	}
	return false
}

func advanceTreeCursorVisible(crsr *Cursor) bool {
	if crsr == nil {
		return false
	}
	pcur := ensurePcur(crsr)
	if pcur == nil || pcur.Cur == nil {
		return false
	}
	for pcur.Cur.Valid() {
		crsr.treeCur = pcur.Cur.Cursor
		rowID, tuple, ok := cursorRow(crsr)
		if ok && tuple != nil {
			recordKey := []byte(nil)
			if crsr.Index == nil && crsr.treeCur != nil {
				recordKey = crsr.treeCur.Key()
			}
			if _, ok := cursorVisibleTuple(crsr, rowID, tuple, recordKey); ok {
				crsr.lastKey = crsr.treeCur.Key()
				return true
			}
		}
		if !pcur.Cur.Next() {
			crsr.treeCur = nil
			return false
		}
	}
	crsr.treeCur = nil
	return false
}

func cursorPageKey(crsr *Cursor, tuple *data.Tuple, rowID uint64, recordKey []byte) []byte {
	if crsr == nil || crsr.Table == nil || crsr.Table.Store == nil {
		return recordKey
	}
	store := crsr.Table.Store
	if len(store.PrimaryKeyFields) == 0 && store.PrimaryKey < 0 {
		if tuple != nil && rowID != 0 {
			return store.KeyForRowID(tuple, rowID)
		}
		if rowID != 0 {
			if rowTuple := store.RowByID(rowID); rowTuple != nil {
				return store.KeyForRow(rowTuple)
			}
		}
	}
	if len(recordKey) > 0 {
		return recordKey
	}
	if tuple != nil && rowID != 0 {
		return store.KeyForRowID(tuple, rowID)
	}
	return recordKey
}

func cursorVisibleTuple(crsr *Cursor, rowID uint64, tuple *data.Tuple, recordKey []byte) (*data.Tuple, bool) {
	if crsr == nil || crsr.Trx == nil || crsr.Trx.ReadView == nil || crsr.Table == nil || crsr.Table.Store == nil {
		return tuple, tuple != nil
	}
	key := cursorPageKey(crsr, tuple, rowID, recordKey)
	if len(key) == 0 {
		return tuple, tuple != nil
	}
	visible, ok := crsr.Table.Store.VersionForView(key, crsr.Trx.ReadView)
	if ok {
		if visible == nil {
			return nil, false
		}
		return visible, true
	}
	return tuple, tuple != nil
}

func decodeCursorValue(crsr *Cursor, value []byte) (uint64, *data.Tuple, bool) {
	rowID, ok := row.DecodeRowID(value)
	if !ok {
		return 0, nil, false
	}
	if tuple, ok := decodeCursorTuple(crsr, value); ok {
		return rowID, tuple, true
	}
	if crsr != nil && crsr.Table != nil && crsr.Table.Store != nil {
		if rowTuple := crsr.Table.Store.RowByID(rowID); rowTuple != nil {
			return rowID, rowTuple, true
		}
	}
	return rowID, nil, false
}

func decodeCursorTuple(crsr *Cursor, value []byte) (*data.Tuple, bool) {
	if len(value) == 0 {
		return nil, false
	}
	recBytes := value
	if len(value) >= 8 {
		recBytes = value[8:]
	}
	if len(recBytes) == 0 {
		return nil, false
	}
	nFields := 0
	if crsr != nil && crsr.Table != nil && crsr.Table.Schema != nil {
		nFields = len(crsr.Table.Schema.Columns)
	}
	if nFields == 0 && crsr != nil && crsr.Table != nil && crsr.Table.Store != nil {
		if len(crsr.Table.Store.Rows) > 0 && crsr.Table.Store.Rows[0] != nil {
			nFields = len(crsr.Table.Store.Rows[0].Fields)
		}
	}
	if nFields == 0 {
		return nil, false
	}
	decoded, err := rec.DecodeVar(recBytes, nFields, 0)
	if err != nil {
		return nil, false
	}
	return decoded, true
}

// CursorMoveTo positions the cursor based on a search tuple.
func CursorMoveTo(crsr *Cursor, tpl *data.Tuple, mode CursorMode, ret *int) ErrCode {
	if crsr == nil || crsr.Table == nil || tpl == nil {
		return DB_ERROR
	}
	crsr.virtualRow = nil
	crsr.pageCur = nil
	keyFields := searchFieldCount(tpl)
	if keyFields == 0 {
		return DB_ERROR
	}
	exactRequired := crsr.MatchMode == IB_EXACT_MATCH
	prefixRequired := crsr.MatchMode == IB_EXACT_PREFIX
	if crsr.Table.Store == nil {
		return DB_ERROR
	}
	store := crsr.Table.Store
	var pkFields int
	var searchKey []byte
	var cols []int
	var prefixes []int
	if crsr.Index != nil {
		cols = crsr.Index.Fields
		prefixes = crsr.Index.Prefixes
		if keyFields > len(cols) {
			keyFields = len(cols)
		}
		if keyFields == 0 {
			return DB_ERROR
		}
		pkFields = len(cols)
		searchKey = store.KeyForSecondarySearch(crsr.Index, tpl, keyFields)
	} else {
		pkFields = primaryKeyCols(crsr.Table)
		if pkFields > 0 && keyFields > pkFields {
			keyFields = pkFields
		}
		searchKey = store.KeyForSearch(tpl, keyFields)
	}
	if len(searchKey) == 0 {
		return DB_ERROR
	}
	if crsr.Index != nil && store != nil {
		if err := store.MergeSecondaryIndexBuffer(crsr.Index); err != nil {
			return DB_ERROR
		}
	}
	if crsr.usePageTree() && crsr.Index == nil && storeHasPrimaryKey(store) {
		cur, exact, err := store.PageTree.Seek(searchKey, btr.SearchGE)
		if err != nil {
			return DB_ERROR
		}
		if cur == nil || !cur.Valid() {
			if exactRequired && assignVirtualRow(crsr, searchKey, keyFields, pkFields, ret) {
				return DB_SUCCESS
			}
			return cursorMoveNotFound(crsr, searchKey)
		}
		if mode == CursorG && exact && bytes.Equal(cur.Key(), searchKey) {
			if !cur.Next() {
				return cursorMoveNotFound(crsr, searchKey)
			}
		}
		crsr.pageCur = cur
		crsr.treeCur = nil
		for crsr.pageCur != nil && crsr.pageCur.Valid() {
			_, rowTuple, ok := cursorPageTuple(crsr)
			if !ok {
				if !crsr.pageCur.Next() {
					break
				}
				continue
			}
			cmp := 0
			if crsr.Index != nil {
				cmp = compareIndexTuplePrefix(rowTuple, tpl, cols, keyFields)
			} else {
				cmp = compareTuplePrefix(rowTuple, tpl, keyFields)
			}
			switch mode {
			case CursorGE:
				if cmp < 0 {
					if !crsr.pageCur.Next() {
						return cursorMoveNotFound(crsr, searchKey)
					}
					continue
				}
			case CursorG:
				if cmp <= 0 {
					if !crsr.pageCur.Next() {
						return cursorMoveNotFound(crsr, searchKey)
					}
					continue
				}
			}
			if prefixRequired {
				if crsr.Index != nil {
					if !tupleHasIndexPrefix(rowTuple, tpl, cols, prefixes, keyFields) {
						if !crsr.pageCur.Next() {
							return cursorMoveNotFound(crsr, searchKey)
						}
						continue
					}
				} else if !tupleHasPrefix(rowTuple, tpl, keyFields) {
					if !crsr.pageCur.Next() {
						return cursorMoveNotFound(crsr, searchKey)
					}
					continue
				}
			}
			if exactRequired {
				if cmp != 0 || (pkFields > 0 && keyFields != pkFields) {
					if !crsr.pageCur.Next() {
						return cursorMoveNotFound(crsr, searchKey)
					}
					continue
				}
			}
			crsr.lastKey = crsr.pageCur.Key()
			if ret != nil {
				if cmp == 0 && (pkFields == 0 || keyFields == pkFields) {
					*ret = 0
				} else {
					*ret = -1
				}
			}
			return DB_SUCCESS
		}
		if exactRequired && assignVirtualRow(crsr, searchKey, keyFields, pkFields, ret) {
			return DB_SUCCESS
		}
		return cursorMoveNotFound(crsr, searchKey)
	}
	if crsr.Tree == nil {
		return DB_ERROR
	}
	pcur := ensurePcur(crsr)
	if pcur == nil || pcur.Cur == nil {
		return DB_ERROR
	}
	if !pcur.Cur.Search(searchKey, btr.SearchGE) {
		if exactRequired && crsr.Index == nil && assignVirtualRow(crsr, searchKey, keyFields, pkFields, ret) {
			return DB_SUCCESS
		}
		return cursorMoveNotFound(crsr, searchKey)
	}
	for pcur.Cur.Valid() {
		rowID, ok := row.DecodeRowID(pcur.Cur.Value())
		if !ok {
			if !pcur.Cur.Next() {
				break
			}
			continue
		}
		rowTuple := store.RowByID(rowID)
		if rowTuple == nil {
			if !pcur.Cur.Next() {
				break
			}
			continue
		}
		cmp := 0
		if crsr.Index != nil {
			cmp = compareIndexTuplePrefix(rowTuple, tpl, cols, keyFields)
		} else {
			cmp = compareTuplePrefix(rowTuple, tpl, keyFields)
		}
		switch mode {
		case CursorGE:
			if cmp < 0 {
				if !pcur.Cur.Next() {
					return cursorMoveNotFound(crsr, searchKey)
				}
				continue
			}
		case CursorG:
			if cmp <= 0 {
				if !pcur.Cur.Next() {
					return cursorMoveNotFound(crsr, searchKey)
				}
				continue
			}
		}
		if prefixRequired {
			if crsr.Index != nil {
				if !tupleHasIndexPrefix(rowTuple, tpl, cols, prefixes, keyFields) {
					if !pcur.Cur.Next() {
						return cursorMoveNotFound(crsr, searchKey)
					}
					continue
				}
			} else if !tupleHasPrefix(rowTuple, tpl, keyFields) {
				if !pcur.Cur.Next() {
					return cursorMoveNotFound(crsr, searchKey)
				}
				continue
			}
		}
		if exactRequired {
			if cmp != 0 || (pkFields > 0 && keyFields != pkFields) {
				if !pcur.Cur.Next() {
					return cursorMoveNotFound(crsr, searchKey)
				}
				continue
			}
		}
		crsr.treeCur = pcur.Cur.Cursor
		crsr.lastKey = pcur.Cur.Key()
		if ret != nil {
			if cmp == 0 && (pkFields == 0 || keyFields == pkFields) {
				*ret = 0
			} else {
				*ret = -1
			}
		}
		return DB_SUCCESS
	}
	if exactRequired && crsr.Index == nil && assignVirtualRow(crsr, searchKey, keyFields, pkFields, ret) {
		return DB_SUCCESS
	}
	return cursorMoveNotFound(crsr, searchKey)
}

func cursorMoveNotFound(crsr *Cursor, searchKey []byte) ErrCode {
	if err := lockGapForKey(crsr, searchKey); err != DB_SUCCESS {
		return err
	}
	return DB_RECORD_NOT_FOUND
}

func assignVirtualRow(crsr *Cursor, searchKey []byte, keyFields, pkFields int, ret *int) bool {
	if crsr == nil || crsr.Table == nil || crsr.Table.Store == nil {
		return false
	}
	if pkFields > 0 && keyFields != pkFields {
		return false
	}
	view := (*read.ReadView)(nil)
	if crsr.Trx != nil {
		view = crsr.Trx.ReadView
	}
	visible, ok := crsr.Table.Store.VersionForView(searchKey, view)
	if !ok || visible == nil {
		return false
	}
	crsr.virtualRow = visible
	crsr.lastKey = searchKey
	crsr.treeCur = nil
	if ret != nil {
		*ret = 0
	}
	return true
}

func ensurePcur(crsr *Cursor) *btr.Pcur {
	if crsr == nil || crsr.Tree == nil {
		return nil
	}
	if crsr.pcur == nil {
		crsr.pcur = btr.NewPcur(crsr.Tree)
	} else if crsr.pcur.Cur == nil {
		crsr.pcur.Cur = btr.NewCur(crsr.Tree)
	}
	return crsr.pcur
}

func searchFieldCount(tpl *data.Tuple) int {
	if tpl == nil {
		return 0
	}
	for i := 0; i < len(tpl.Fields); i++ {
		field := tpl.Fields[i]
		if field.Len == data.UnivSQLNull {
			return i + 1
		}
		if field.Len == 0 && len(field.Data) == 0 {
			return i
		}
	}
	return len(tpl.Fields)
}

func primaryKeyCols(table *Table) int {
	if table == nil || table.Schema == nil {
		return 0
	}
	for _, idx := range table.Schema.Indexes {
		if idx != nil && idx.Clustered {
			return len(idx.Columns)
		}
	}
	return 0
}

func storeHasPrimaryKey(store *row.Store) bool {
	if store == nil {
		return false
	}
	return len(store.PrimaryKeyFields) > 0 || store.PrimaryKey >= 0
}

func compareTuplePrefix(row, search *data.Tuple, n int) int {
	if row == nil || search == nil {
		switch {
		case row == search:
			return 0
		case row == nil:
			return -1
		default:
			return 1
		}
	}
	if n > len(row.Fields) {
		n = len(row.Fields)
	}
	if n > len(search.Fields) {
		n = len(search.Fields)
	}
	for i := 0; i < n; i++ {
		cmp := data.CompareFields(&row.Fields[i], &search.Fields[i])
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

func compareIndexTuplePrefix(row, search *data.Tuple, cols []int, n int) int {
	if row == nil || search == nil {
		switch {
		case row == search:
			return 0
		case row == nil:
			return -1
		default:
			return 1
		}
	}
	if n > len(cols) {
		n = len(cols)
	}
	for i := 0; i < n; i++ {
		col := cols[i]
		if col < 0 || col >= len(row.Fields) || col >= len(search.Fields) {
			return 0
		}
		cmp := data.CompareFields(&row.Fields[col], &search.Fields[col])
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

func tupleHasPrefix(row, search *data.Tuple, n int) bool {
	if row == nil || search == nil {
		return false
	}
	if n > len(row.Fields) {
		n = len(row.Fields)
	}
	if n > len(search.Fields) {
		n = len(search.Fields)
	}
	for i := 0; i < n; i++ {
		if !fieldHasPrefix(row.Fields[i], search.Fields[i]) {
			return false
		}
	}
	return true
}

func tupleHasIndexPrefix(row, search *data.Tuple, cols []int, prefixes []int, n int) bool {
	if row == nil || search == nil {
		return false
	}
	if n > len(cols) {
		n = len(cols)
	}
	for i := 0; i < n; i++ {
		col := cols[i]
		if col < 0 || col >= len(row.Fields) || col >= len(search.Fields) {
			return false
		}
		prefix := 0
		if i < len(prefixes) {
			prefix = prefixes[i]
		}
		if !fieldHasPrefixLen(row.Fields[col], search.Fields[col], prefix) {
			return false
		}
	}
	return true
}

func fieldHasPrefix(row, search data.Field) bool {
	if search.Len == data.UnivSQLNull {
		return row.Len == data.UnivSQLNull
	}
	if search.Len == 0 && len(search.Data) == 0 {
		return true
	}
	if row.Len == data.UnivSQLNull {
		return false
	}
	slen := int(search.Len)
	if slen > len(search.Data) {
		slen = len(search.Data)
	}
	if slen > len(row.Data) {
		return false
	}
	return bytes.Equal(row.Data[:slen], search.Data[:slen])
}

func fieldHasPrefixLen(row, search data.Field, prefix int) bool {
	if prefix <= 0 {
		return fieldHasPrefix(row, search)
	}
	if search.Len == data.UnivSQLNull {
		return row.Len == data.UnivSQLNull
	}
	if search.Len == 0 && len(search.Data) == 0 {
		return true
	}
	if row.Len == data.UnivSQLNull {
		return false
	}
	slen := int(search.Len)
	if slen > len(search.Data) {
		slen = len(search.Data)
	}
	if prefix < slen {
		slen = prefix
	}
	if slen > len(row.Data) {
		return false
	}
	return bytes.Equal(row.Data[:slen], search.Data[:slen])
}

// ClustReadTupleCreate allocates a read tuple for the cursor.
func ClustReadTupleCreate(crsr *Cursor) *data.Tuple {
	return newTupleForCursor(crsr)
}

// ClustSearchTupleCreate allocates a search tuple for the cursor.
func ClustSearchTupleCreate(crsr *Cursor) *data.Tuple {
	return newTupleForCursor(crsr)
}

// TupleWriteI32 writes an int32 value into a tuple.
func TupleWriteI32(tpl *data.Tuple, col int, val int32) ErrCode {
	if tpl == nil || col < 0 || col >= len(tpl.Fields) {
		return DB_ERROR
	}
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(val))
	tpl.Fields[col].Data = append([]byte(nil), buf[:]...)
	tpl.Fields[col].Len = 4
	return DB_SUCCESS
}

// TupleReadI32 reads an int32 value from a tuple.
func TupleReadI32(tpl *data.Tuple, col int, out *int32) ErrCode {
	if tpl == nil || out == nil || col < 0 || col >= len(tpl.Fields) {
		return DB_ERROR
	}
	field := tpl.Fields[col]
	if len(field.Data) < 4 {
		return DB_ERROR
	}
	*out = int32(binary.BigEndian.Uint32(field.Data))
	return DB_SUCCESS
}

// TupleClear resets tuple fields.
func TupleClear(tpl *data.Tuple) *data.Tuple {
	if tpl == nil {
		return nil
	}
	for i := range tpl.Fields {
		tpl.Fields[i].Data = nil
		tpl.Fields[i].Len = 0
	}
	return tpl
}

// TupleDelete releases a tuple.
func TupleDelete(tpl *data.Tuple) {
	unregisterTupleMeta(tpl)
}

func newTupleForCursor(crsr *Cursor) *data.Tuple {
	n := 0
	if crsr != nil && crsr.Table != nil && crsr.Table.Schema != nil {
		n = len(crsr.Table.Schema.Columns)
	}
	if n == 0 {
		n = 1
	}
	fields := make([]data.Field, n)
	tpl := &data.Tuple{
		NFields:    n,
		NFieldsCmp: n,
		Fields:     fields,
		Magic:      data.DataTupleMagic,
	}
	if crsr != nil && crsr.Table != nil && crsr.Table.Schema != nil {
		registerTupleMeta(tpl, crsr.Table.Schema)
	}
	return tpl
}

func tupleReadI32(tpl *data.Tuple, col int) (int32, ErrCode) {
	var out int32
	if err := TupleReadI32(tpl, col, &out); err != DB_SUCCESS {
		return 0, err
	}
	return out, DB_SUCCESS
}

func copyTuple(dst, src *data.Tuple) {
	if dst == nil || src == nil {
		return
	}
	dst.InfoBits = src.InfoBits
	dst.Magic = src.Magic
	if len(dst.Fields) != len(src.Fields) {
		dst.Fields = make([]data.Field, len(src.Fields))
		dst.NFields = len(src.Fields)
		dst.NFieldsCmp = len(src.Fields)
	}
	for i := range src.Fields {
		srcField := src.Fields[i]
		dataBytes := append([]byte(nil), srcField.Data...)
		dst.Fields[i] = data.Field{
			Data: dataBytes,
			Len:  srcField.Len,
			Ext:  srcField.Ext,
			Type: srcField.Type,
		}
	}
}

func cloneTuple(src *data.Tuple) *data.Tuple {
	if src == nil {
		return nil
	}
	dst := &data.Tuple{
		InfoBits:   src.InfoBits,
		NFields:    len(src.Fields),
		NFieldsCmp: src.NFieldsCmp,
		Magic:      src.Magic,
		Fields:     make([]data.Field, len(src.Fields)),
	}
	copyTuple(dst, src)
	return dst
}

func findTable(name string) *Table {
	schemaMu.Lock()
	defer schemaMu.Unlock()
	for _, db := range databases {
		if table := db.Tables[strings.ToLower(name)]; table != nil {
			return table
		}
	}
	return nil
}
