package api

import (
	"bytes"

	"github.com/wilhasse/innodb-go/data"
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
	store := crsr.Table.Store
	index := -1
	if len(store.PrimaryKeyFields) > 0 {
		for i, row := range store.Rows {
			if row == nil {
				continue
			}
			if tupleKeyEqual(row, oldTpl, store.PrimaryKeyFields, store.PrimaryKeyPrefixes) {
				index = i
				break
			}
		}
	} else if store.PrimaryKey >= 0 && store.PrimaryKey < len(oldTpl.Fields) {
		keyField := oldTpl.Fields[store.PrimaryKey]
		for i, row := range store.Rows {
			if row == nil || store.PrimaryKey >= len(row.Fields) {
				continue
			}
			if fieldEqualPrefix(keyField, row.Fields[store.PrimaryKey], store.PrimaryKeyPrefix) {
				index = i
				break
			}
		}
	} else {
		for i, row := range store.Rows {
			if tupleEqual(row, oldTpl) {
				index = i
				break
			}
		}
	}
	if index < 0 {
		return DB_RECORD_NOT_FOUND
	}
	store.Rows[index] = cloneTuple(newTpl)
	return DB_SUCCESS
}

// CursorDeleteRow deletes the row at the current cursor position.
func CursorDeleteRow(crsr *Cursor) ErrCode {
	if crsr == nil || crsr.Table == nil || crsr.Table.Store == nil {
		return DB_ERROR
	}
	if crsr.pos < 0 || crsr.pos >= len(crsr.Table.Store.Rows) {
		return DB_RECORD_NOT_FOUND
	}
	rows := crsr.Table.Store.Rows
	copy(rows[crsr.pos:], rows[crsr.pos+1:])
	rows = rows[:len(rows)-1]
	crsr.Table.Store.Rows = rows
	if crsr.pos >= len(rows) {
		crsr.pos = len(rows) - 1
	}
	return DB_SUCCESS
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
