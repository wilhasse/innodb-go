package row

import (
	"bytes"

	"github.com/wilhasse/innodb-go/data"
)

func (store *Store) insertPageTree(key, value []byte) error {
	if store == nil || store.PageTree == nil {
		return nil
	}
	_, err := store.PageTree.Insert(key, value)
	return err
}

func (store *Store) updatePageTree(oldKey, newKey, value []byte) error {
	if store == nil || store.PageTree == nil {
		return nil
	}
	if bytes.Equal(oldKey, newKey) {
		_, err := store.PageTree.Insert(newKey, value)
		return err
	}
	if _, err := store.PageTree.Insert(newKey, value); err != nil {
		return err
	}
	if _, err := store.PageTree.Delete(oldKey); err != nil {
		_, _ = store.PageTree.Delete(newKey)
		return err
	}
	return nil
}

func (store *Store) deletePageTree(key []byte) error {
	if store == nil || store.PageTree == nil || len(key) == 0 {
		return nil
	}
	_, err := store.PageTree.Delete(key)
	return err
}

func (store *Store) pageTreeKey(row *data.Tuple, rowID uint64) []byte {
	if store == nil || row == nil {
		return nil
	}
	switch {
	case len(store.PrimaryKeyFields) > 0:
		return buildKey(row, store.PrimaryKeyFields, store.PrimaryKeyPrefixes, len(store.PrimaryKeyFields), 0, false)
	case store.PrimaryKey >= 0:
		cols := []int{store.PrimaryKey}
		prefixes := []int{store.PrimaryKeyPrefix}
		return buildKey(row, cols, prefixes, 1, 0, false)
	default:
		var buf bytes.Buffer
		appendRowIDKey(&buf, rowID)
		return buf.Bytes()
	}
}
