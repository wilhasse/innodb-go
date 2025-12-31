package row

import (
	"bytes"
	"errors"
	"sync"

	"github.com/wilhasse/innodb-go/btr"
	"github.com/wilhasse/innodb-go/data"
	ibos "github.com/wilhasse/innodb-go/os"
)

// ErrDuplicateKey reports a duplicate primary key insertion.
var ErrDuplicateKey = errors.New("row: duplicate key")

// Store holds rows for a table.
type Store struct {
	Rows               []*data.Tuple
	PrimaryKey         int
	PrimaryKeyPrefix   int
	PrimaryKeyFields   []int
	PrimaryKeyPrefixes []int
	Tree               *btr.Tree
	file               ibos.File
	filePath           string
	fileOffset         int64
	nextRowID          uint64
	rowsByID           map[uint64]*data.Tuple
	idByRow            map[*data.Tuple]uint64
	mu                 sync.RWMutex
}

// NewStore creates a row store with a primary key field index.
func NewStore(primaryKey int) *Store {
	store := &Store{PrimaryKey: primaryKey}
	store.ensureIndex()
	return store
}

// Insert adds a tuple, enforcing primary key uniqueness when configured.
func (store *Store) Insert(tuple *data.Tuple) error {
	if store == nil || tuple == nil {
		return errors.New("row: nil store or tuple")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.hasDuplicate(tuple) {
		return ErrDuplicateKey
	}
	store.ensureIndex()
	id := store.nextRowID
	store.nextRowID++
	store.Rows = append(store.Rows, tuple)
	if store.rowsByID != nil {
		store.rowsByID[id] = tuple
	}
	if store.idByRow != nil {
		store.idByRow[tuple] = id
	}
	if store.Tree != nil {
		key := store.keyForInsert(tuple, id)
		val := encodeRowValue(id, tuple)
		cur := btr.NewCur(store.Tree)
		if !cur.OptimisticInsert(key, val) {
			store.Tree.Insert(key, val)
		}
		store.appendLog(storeOpInsert, key, val)
	}
	return nil
}

func (store *Store) hasDuplicate(tuple *data.Tuple) bool {
	if store == nil || tuple == nil {
		return false
	}
	if len(store.PrimaryKeyFields) > 0 {
		return store.hasCompositeKey(tuple)
	}
	if store.PrimaryKey >= 0 && store.PrimaryKey < len(tuple.Fields) {
		return store.hasSingleKey(tuple.Fields[store.PrimaryKey])
	}
	return false
}

func (store *Store) hasCompositeKey(tuple *data.Tuple) bool {
	for _, row := range store.Rows {
		if row == nil {
			continue
		}
		if compositeFieldsEqual(row, tuple, store.PrimaryKeyFields, store.PrimaryKeyPrefixes) {
			return true
		}
	}
	return false
}

func (store *Store) hasSingleKey(field data.Field) bool {
	for _, row := range store.Rows {
		if row == nil || store.PrimaryKey < 0 || store.PrimaryKey >= len(row.Fields) {
			continue
		}
		if fieldsEqualPrefix(field, row.Fields[store.PrimaryKey], store.PrimaryKeyPrefix) {
			return true
		}
	}
	return false
}

func compositeFieldsEqual(a, b *data.Tuple, cols []int, prefixes []int) bool {
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
		if !fieldsEqualPrefix(a.Fields[col], b.Fields[col], prefix) {
			return false
		}
	}
	return true
}

func fieldsEqualPrefix(a, b data.Field, prefix int) bool {
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
