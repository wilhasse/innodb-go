package row

import (
	"bytes"
	"encoding/binary"

	"github.com/wilhasse/innodb-go/btr"
	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/rec"
)

const storeTreeOrder = 4

// CompareKeys compares encoded row keys field-by-field.
func CompareKeys(a, b []byte) int {
	ia, ib := 0, 0
	for {
		if ia >= len(a) || ib >= len(b) {
			switch {
			case ia >= len(a) && ib >= len(b):
				return 0
			case ia >= len(a):
				return -1
			default:
				return 1
			}
		}
		if ia+4 > len(a) || ib+4 > len(b) {
			return bytes.Compare(a, b)
		}
		la := binary.BigEndian.Uint32(a[ia:])
		lb := binary.BigEndian.Uint32(b[ib:])
		ia += 4
		ib += 4

		aNull := la == data.UnivSQLNull
		bNull := lb == data.UnivSQLNull
		if aNull || bNull {
			switch {
			case aNull && bNull:
				continue
			case aNull:
				return -1
			default:
				return 1
			}
		}

		alen := int(la)
		blen := int(lb)
		if ia+alen > len(a) {
			alen = len(a) - ia
		}
		if ib+blen > len(b) {
			blen = len(b) - ib
		}
		cmp := bytes.Compare(a[ia:ia+alen], b[ib:ib+blen])
		if cmp != 0 {
			return cmp
		}
		if la != lb {
			if la < lb {
				return -1
			}
			return 1
		}
		ia += alen
		ib += blen
	}
}

func encodeRowID(id uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], id)
	return buf[:]
}

func encodeRowValue(id uint64, tuple *data.Tuple) []byte {
	recBytes, err := rec.EncodeVar(tuple, nil, 0)
	if err != nil {
		return encodeRowID(id)
	}
	val := make([]byte, 8+len(recBytes))
	binary.BigEndian.PutUint64(val[:8], id)
	copy(val[8:], recBytes)
	return val
}

// DecodeRowID converts a stored row ID value into uint64.
func DecodeRowID(value []byte) (uint64, bool) {
	if len(value) < 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(value[:8]), true
}

// ensureIndex assumes store.mu is held.
func (store *Store) ensureIndex() {
	if store == nil {
		return
	}
	if store.rowsByID == nil || store.idByRow == nil || store.Tree == nil {
		store.rebuildIndex()
	}
}

func (store *Store) rebuildIndex() {
	if store == nil {
		return
	}
	store.Tree = btr.NewTree(storeTreeOrder, CompareKeys)
	store.rowsByID = make(map[uint64]*data.Tuple)
	store.idByRow = make(map[*data.Tuple]uint64)
	store.nextRowID = 1
	for _, row := range store.Rows {
		if row == nil {
			continue
		}
		id := store.nextRowID
		store.nextRowID++
		store.rowsByID[id] = row
		store.idByRow[row] = id
		key := store.keyForInsert(row, id)
		store.Tree.Insert(key, encodeRowValue(id, row))
	}
}

// Reset clears rows and rebuilds the index state.
func (store *Store) Reset() {
	if store == nil {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.Rows = nil
	_ = store.TruncateFile()
	store.rebuildIndex()
}

// RowID returns the internal row ID for a tuple.
func (store *Store) RowID(row *data.Tuple) (uint64, bool) {
	if store == nil || row == nil {
		return 0, false
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.ensureIndex()
	id, ok := store.idByRow[row]
	return id, ok
}

// RowByID returns a tuple by row ID.
func (store *Store) RowByID(id uint64) *data.Tuple {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.ensureIndex()
	return store.rowsByID[id]
}

func (store *Store) removeTuple(row *data.Tuple) bool {
	if store == nil || row == nil {
		return false
	}
	store.ensureIndex()
	id, ok := store.idByRow[row]
	if ok {
		delete(store.idByRow, row)
		delete(store.rowsByID, id)
		if store.Tree != nil {
			key := store.keyForInsert(row, id)
			cur := btr.NewCur(store.Tree)
			if cur.Search(key, btr.SearchGE) && CompareKeys(cur.Key(), key) == 0 {
				cur.OptimisticDelete()
			} else {
				store.Tree.Delete(key)
			}
			store.appendLog(storeOpDelete, key, nil)
		}
	}
	for i, existing := range store.Rows {
		if existing == row {
			store.Rows = append(store.Rows[:i], store.Rows[i+1:]...)
			return true
		}
	}
	return false
}

// RemoveTuple deletes a tuple from the store.
func (store *Store) RemoveTuple(row *data.Tuple) bool {
	if store == nil {
		return false
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.ensureIndex()
	return store.removeTuple(row)
}

func (store *Store) replaceTuple(oldRow, newRow *data.Tuple) error {
	if store == nil || oldRow == nil || newRow == nil {
		return ErrRowNotFound
	}
	store.ensureIndex()
	id, ok := store.idByRow[oldRow]
	if !ok {
		return ErrRowNotFound
	}
	oldKey := store.keyForInsert(oldRow, id)
	newKey := store.keyForInsert(newRow, id)
	if !bytes.Equal(oldKey, newKey) && store.hasDuplicateExcept(newRow, oldRow) {
		return ErrDuplicateKey
	}
	for i, existing := range store.Rows {
		if existing == oldRow {
			store.Rows[i] = newRow
			break
		}
	}
	delete(store.idByRow, oldRow)
	store.idByRow[newRow] = id
	store.rowsByID[id] = newRow
	if store.Tree != nil {
		if !bytes.Equal(oldKey, newKey) {
			cur := btr.NewCur(store.Tree)
			if cur.Search(oldKey, btr.SearchGE) && CompareKeys(cur.Key(), oldKey) == 0 {
				cur.OptimisticDelete()
			} else {
				store.Tree.Delete(oldKey)
			}
		}
		val := encodeRowValue(id, newRow)
		cur := btr.NewCur(store.Tree)
		if !cur.OptimisticInsert(newKey, val) {
			store.Tree.Insert(newKey, val)
		}
	}
	store.appendLog(storeOpUpdate, newKey, encodeRowValue(id, newRow))
	return nil
}

// ReplaceTuple updates a tuple in the store, adjusting the index if needed.
func (store *Store) ReplaceTuple(oldRow, newRow *data.Tuple) error {
	if store == nil {
		return ErrRowNotFound
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.ensureIndex()
	return store.replaceTuple(oldRow, newRow)
}

func (store *Store) hasDuplicateExcept(tuple, exclude *data.Tuple) bool {
	if store == nil || tuple == nil {
		return false
	}
	if len(store.PrimaryKeyFields) > 0 {
		for _, row := range store.Rows {
			if row == nil || row == exclude {
				continue
			}
			if compositeFieldsEqual(row, tuple, store.PrimaryKeyFields, store.PrimaryKeyPrefixes) {
				return true
			}
		}
		return false
	}
	if store.PrimaryKey >= 0 && store.PrimaryKey < len(tuple.Fields) {
		keyField := tuple.Fields[store.PrimaryKey]
		for _, row := range store.Rows {
			if row == nil || row == exclude || store.PrimaryKey >= len(row.Fields) {
				continue
			}
			if fieldsEqualPrefix(keyField, row.Fields[store.PrimaryKey], store.PrimaryKeyPrefix) {
				return true
			}
		}
	}
	return false
}

func (store *Store) keyForInsert(tuple *data.Tuple, rowID uint64) []byte {
	if store == nil || tuple == nil {
		return nil
	}
	var cols []int
	var prefixes []int
	fieldCount := 0
	includeRowID := false
	switch {
	case len(store.PrimaryKeyFields) > 0:
		cols = store.PrimaryKeyFields
		prefixes = store.PrimaryKeyPrefixes
		fieldCount = len(cols)
	case store.PrimaryKey >= 0:
		cols = []int{store.PrimaryKey}
		prefixes = []int{store.PrimaryKeyPrefix}
		fieldCount = 1
	default:
		fieldCount = len(tuple.Fields)
		includeRowID = true
	}
	return buildKey(tuple, cols, prefixes, fieldCount, rowID, includeRowID)
}

// KeyForSearch builds an encoded key from the search tuple.
func (store *Store) KeyForSearch(tuple *data.Tuple, fieldCount int) []byte {
	if store == nil || tuple == nil || fieldCount <= 0 {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.ensureIndex()
	cols, prefixes := store.keyColumns(fieldCount)
	return buildKey(tuple, cols, prefixes, fieldCount, 0, false)
}

func (store *Store) keyColumns(fieldCount int) ([]int, []int) {
	if store == nil || fieldCount <= 0 {
		return nil, nil
	}
	if len(store.PrimaryKeyFields) > 0 {
		if fieldCount > len(store.PrimaryKeyFields) {
			fieldCount = len(store.PrimaryKeyFields)
		}
		return store.PrimaryKeyFields[:fieldCount], store.PrimaryKeyPrefixes[:fieldCount]
	}
	if store.PrimaryKey >= 0 {
		if fieldCount > 1 {
			fieldCount = 1
		}
		return []int{store.PrimaryKey}[:fieldCount], []int{store.PrimaryKeyPrefix}[:fieldCount]
	}
	cols := make([]int, 0, fieldCount)
	prefixes := make([]int, 0, fieldCount)
	for i := 0; i < fieldCount; i++ {
		cols = append(cols, i)
		prefixes = append(prefixes, 0)
	}
	return cols, prefixes
}

func buildKey(tuple *data.Tuple, cols []int, prefixes []int, fieldCount int, rowID uint64, includeRowID bool) []byte {
	if tuple == nil {
		return nil
	}
	var buf bytes.Buffer
	if fieldCount > len(cols) {
		fieldCount = len(cols)
	}
	for i := 0; i < fieldCount; i++ {
		col := cols[i]
		prefix := 0
		if i < len(prefixes) {
			prefix = prefixes[i]
		}
		field := data.Field{Len: data.UnivSQLNull}
		if col >= 0 && col < len(tuple.Fields) {
			field = tuple.Fields[col]
		}
		appendFieldKey(&buf, field, prefix)
	}
	if includeRowID {
		appendRowIDKey(&buf, rowID)
	}
	return buf.Bytes()
}

func appendFieldKey(buf *bytes.Buffer, field data.Field, prefix int) {
	if buf == nil {
		return
	}
	if field.Len == data.UnivSQLNull {
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], data.UnivSQLNull)
		buf.Write(lenBuf[:])
		return
	}
	length := int(field.Len)
	if length > len(field.Data) {
		length = len(field.Data)
	}
	if prefix > 0 && length > prefix {
		length = prefix
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(length))
	buf.Write(lenBuf[:])
	if length > 0 {
		buf.Write(field.Data[:length])
	}
}

func appendRowIDKey(buf *bytes.Buffer, rowID uint64) {
	if buf == nil {
		return
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], 8)
	buf.Write(lenBuf[:])
	var idBuf [8]byte
	binary.BigEndian.PutUint64(idBuf[:], rowID)
	buf.Write(idBuf[:])
}
