package api

import (
	"bytes"
	"errors"

	"github.com/wilhasse/innodb-go/btr"
	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/lock"
	"github.com/wilhasse/innodb-go/rec"
	"github.com/wilhasse/innodb-go/row"
)

// CursorSetLockMode sets the cursor lock mode (stub).
func CursorSetLockMode(_ *Cursor, _ LockMode) ErrCode {
	return DB_SUCCESS
}

// CursorSetMatchMode updates the cursor match mode.
func CursorSetMatchMode(crsr *Cursor, mode MatchMode) ErrCode {
	if crsr == nil {
		return DB_ERROR
	}
	crsr.MatchMode = mode
	return DB_SUCCESS
}

// CursorSetClusterAccess is a no-op for the in-memory cursor.
func CursorSetClusterAccess(_ *Cursor) {
}

// SecSearchTupleCreate allocates a search tuple for a secondary index.
func SecSearchTupleCreate(crsr *Cursor) *data.Tuple {
	return newTupleForCursor(crsr)
}

// TupleCopy copies tuple contents.
func TupleCopy(dst, src *data.Tuple) ErrCode {
	if dst == nil || src == nil {
		return DB_ERROR
	}
	copyTuple(dst, src)
	return DB_SUCCESS
}

// CursorUpdateRow replaces a row matching the old tuple.
func CursorUpdateRow(crsr *Cursor, oldTpl, newTpl *data.Tuple) ErrCode {
	if crsr == nil || crsr.Table == nil || crsr.Table.Store == nil || oldTpl == nil || newTpl == nil {
		return DB_ERROR
	}
	if crsr.treeCur != nil && crsr.treeCur.Valid() {
		crsr.lastKey = crsr.treeCur.Key()
	}
	store := crsr.Table.Store
	target := findRowForUpdate(crsr, oldTpl)
	if target == nil {
		return DB_RECORD_NOT_FOUND
	}
	if err := lockTableForDML(crsr); err != DB_SUCCESS {
		return err
	}
	if err := lockRecordForDML(crsr, target, lock.ModeX); err != DB_SUCCESS {
		return err
	}
	oldKey := primaryKeyBytes(store, target)
	before := encodeUndoImage(target)
	encoded, err := encodeDecodeTuple(newTpl)
	if err != DB_SUCCESS {
		return err
	}
	if err := store.ReplaceTuple(target, encoded); err != nil {
		if errors.Is(err, row.ErrDuplicateKey) {
			return DB_DUPLICATE_KEY
		}
		if errors.Is(err, row.ErrRowNotFound) {
			return DB_RECORD_NOT_FOUND
		}
		return DB_ERROR
	}
	recordUndoUpdate(crsr, encoded, before)
	newKey := primaryKeyBytes(store, encoded)
	if len(oldKey) > 0 && len(newKey) > 0 && !bytes.Equal(oldKey, newKey) {
		recordRowVersionForKey(crsr, oldKey, nil)
	}
	recordRowVersionForKey(crsr, newKey, encoded)
	crsr.treeCur = nil
	if crsr.pcur != nil {
		crsr.pcur.Init()
	}
	return DB_SUCCESS
}

func findRowForUpdate(crsr *Cursor, tpl *data.Tuple) *data.Tuple {
	if crsr == nil || crsr.Table == nil || crsr.Table.Store == nil || crsr.Tree == nil || tpl == nil {
		return nil
	}
	store := crsr.Table.Store
	keyFields := searchFieldCount(tpl)
	if keyFields == 0 {
		return nil
	}
	pkFields := primaryKeyCols(crsr.Table)
	if pkFields > 0 && keyFields > pkFields {
		keyFields = pkFields
	}
	searchKey := store.KeyForSearch(tpl, keyFields)
	if len(searchKey) == 0 {
		return nil
	}
	pcur := ensurePcur(crsr)
	if pcur == nil || pcur.Cur == nil {
		return nil
	}
	if !pcur.Cur.Search(searchKey, btr.SearchGE) {
		return nil
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
		if len(store.PrimaryKeyFields) > 0 {
			if tupleKeyEqual(rowTuple, tpl, store.PrimaryKeyFields, store.PrimaryKeyPrefixes) {
				crsr.treeCur = pcur.Cur.Cursor
				return rowTuple
			}
		} else if store.PrimaryKey >= 0 && store.PrimaryKey < len(tpl.Fields) {
			keyField := tpl.Fields[store.PrimaryKey]
			if store.PrimaryKey < len(rowTuple.Fields) && fieldEqualPrefix(keyField, rowTuple.Fields[store.PrimaryKey], store.PrimaryKeyPrefix) {
				crsr.treeCur = pcur.Cur.Cursor
				return rowTuple
			}
		} else if tupleEqual(rowTuple, tpl) {
			crsr.treeCur = pcur.Cur.Cursor
			return rowTuple
		}
		if !pcur.Cur.Next() {
			break
		}
	}
	return nil
}

// CursorDeleteRow deletes the row at the current cursor position.
func CursorDeleteRow(crsr *Cursor) ErrCode {
	if crsr == nil || crsr.Table == nil || crsr.Table.Store == nil {
		return DB_ERROR
	}
	var row *data.Tuple
	if decoded, ok := decodeCursorRecord(crsr); ok {
		row = findStoredTuple(crsr.Table.Store, decoded)
	}
	if row == nil {
		var ok bool
		row, ok = cursorRow(crsr)
		if !ok {
			return DB_RECORD_NOT_FOUND
		}
	}
	if err := lockTableForDML(crsr); err != DB_SUCCESS {
		return err
	}
	if err := lockRecordForDML(crsr, row, lock.ModeX); err != DB_SUCCESS {
		return err
	}
	if crsr.treeCur != nil && crsr.treeCur.Valid() {
		crsr.lastKey = crsr.treeCur.Key()
	}
	before := encodeUndoImage(row)
	deleteKey := primaryKeyBytes(crsr.Table.Store, row)
	if !crsr.Table.Store.RemoveTuple(row) {
		return DB_RECORD_NOT_FOUND
	}
	recordUndoDelete(crsr, row, before)
	recordRowVersionForKey(crsr, deleteKey, nil)
	crsr.treeCur = nil
	return DB_SUCCESS
}

func decodeCursorRecord(crsr *Cursor) (*data.Tuple, bool) {
	if crsr == nil || crsr.Table == nil || crsr.treeCur == nil || !crsr.treeCur.Valid() {
		return nil, false
	}
	value := crsr.treeCur.Value()
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
	nFields := len(crsr.Table.Schema.Columns)
	if nFields == 0 && crsr.Table.Store != nil {
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

func findStoredTuple(store *row.Store, tpl *data.Tuple) *data.Tuple {
	if store == nil || tpl == nil {
		return nil
	}
	rows := store.SelectWhere(func(candidate *data.Tuple) bool {
		return tupleEqual(candidate, tpl)
	})
	if len(rows) == 0 {
		return nil
	}
	return rows[0]
}

func tupleEqual(a, b *data.Tuple) bool {
	if a == nil || b == nil {
		return false
	}
	if len(a.Fields) != len(b.Fields) {
		return false
	}
	for i := range a.Fields {
		if !fieldEqualPrefix(a.Fields[i], b.Fields[i], 0) {
			return false
		}
	}
	return true
}

func tupleKeyEqual(a, b *data.Tuple, cols []int, prefixes []int) bool {
	if a == nil || b == nil {
		return false
	}
	for i, col := range cols {
		if col < 0 || col >= len(a.Fields) || col >= len(b.Fields) {
			return false
		}
		prefix := 0
		if i < len(prefixes) {
			prefix = prefixes[i]
		}
		if !fieldEqualPrefix(a.Fields[col], b.Fields[col], prefix) {
			return false
		}
	}
	return true
}

func fieldEqualPrefix(a, b data.Field, prefix int) bool {
	if a.Len == data.UnivSQLNull || b.Len == data.UnivSQLNull {
		return a.Len == b.Len
	}
	if prefix <= 0 {
		if a.Len != b.Len {
			return false
		}
		return bytes.Equal(a.Data, b.Data)
	}
	alen := int(a.Len)
	blen := int(b.Len)
	if alen > len(a.Data) {
		alen = len(a.Data)
	}
	if blen > len(b.Data) {
		blen = len(b.Data)
	}
	n := prefix
	if n > alen {
		n = alen
	}
	if n > blen {
		n = blen
	}
	return bytes.Equal(a.Data[:n], b.Data[:n])
}
