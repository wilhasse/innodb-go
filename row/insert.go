package row

import (
	"bytes"
	"errors"

	"github.com/wilhasse/innodb-go/data"
)

// ErrDuplicateKey reports a duplicate primary key insertion.
var ErrDuplicateKey = errors.New("row: duplicate key")

// Store holds rows for a table.
type Store struct {
	Rows             []*data.Tuple
	PrimaryKey       int
	PrimaryKeyPrefix int
}

// NewStore creates a row store with a primary key field index.
func NewStore(primaryKey int) *Store {
	return &Store{PrimaryKey: primaryKey}
}

// Insert adds a tuple, enforcing primary key uniqueness when configured.
func (store *Store) Insert(tuple *data.Tuple) error {
	if store == nil || tuple == nil {
		return errors.New("row: nil store or tuple")
	}
	if store.PrimaryKey >= 0 && store.PrimaryKey < len(tuple.Fields) {
		if store.hasKey(tuple.Fields[store.PrimaryKey]) {
			return ErrDuplicateKey
		}
	}
	store.Rows = append(store.Rows, tuple)
	return nil
}

func (store *Store) hasKey(field data.Field) bool {
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
