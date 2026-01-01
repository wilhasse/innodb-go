package row

import (
	"bytes"
	"errors"
	"strings"

	"github.com/wilhasse/innodb-go/btr"
	"github.com/wilhasse/innodb-go/data"
)

// SecondaryIndex tracks a secondary index tree and column mapping.
type SecondaryIndex struct {
	Name     string
	Fields   []int
	Prefixes []int
	Unique   bool
	Tree     *btr.Tree
}

// SecondaryIndex returns a secondary index by name.
func (store *Store) SecondaryIndex(name string) *SecondaryIndex {
	if store == nil || name == "" {
		return nil
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.SecondaryIndexes == nil {
		return nil
	}
	return store.SecondaryIndexes[strings.ToLower(name)]
}

// RemoveSecondaryIndex removes a secondary index by name.
func (store *Store) RemoveSecondaryIndex(name string) {
	if store == nil || name == "" {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.SecondaryIndexes == nil {
		return
	}
	delete(store.SecondaryIndexes, strings.ToLower(name))
}

// AddSecondaryIndex registers a new secondary index and builds it from rows.
func (store *Store) AddSecondaryIndex(name string, fields []int, prefixes []int, unique bool) error {
	if store == nil {
		return errors.New("row: nil store")
	}
	if len(fields) == 0 {
		return errors.New("row: empty secondary index")
	}
	if prefixes != nil && len(prefixes) != len(fields) {
		return errors.New("row: secondary prefix mismatch")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.ensureIndex()

	key := strings.ToLower(name)
	if key == "" {
		return errors.New("row: empty secondary index name")
	}
	if store.SecondaryIndexes == nil {
		store.SecondaryIndexes = make(map[string]*SecondaryIndex)
	}
	if _, ok := store.SecondaryIndexes[key]; ok {
		return nil
	}
	idx := &SecondaryIndex{
		Name:     name,
		Fields:   append([]int(nil), fields...),
		Prefixes: append([]int(nil), prefixes...),
		Unique:   unique,
		Tree:     btr.NewTree(storeTreeOrder, CompareKeys),
	}
	for id, row := range store.rowsByID {
		if row == nil {
			continue
		}
		keyBytes := store.secondaryKeyForInsert(idx, row, id)
		if len(keyBytes) == 0 {
			continue
		}
		if unique {
			if store.secondaryDuplicate(idx, keyBytes, id) {
				return ErrDuplicateKey
			}
		}
		idx.Tree.Insert(keyBytes, encodeRowID(id))
	}
	store.SecondaryIndexes[key] = idx
	return nil
}

// KeyForSecondarySearch builds an encoded key for a secondary index search.
func (store *Store) KeyForSecondarySearch(index *SecondaryIndex, tuple *data.Tuple, fieldCount int) []byte {
	if store == nil || index == nil || tuple == nil || fieldCount <= 0 {
		return nil
	}
	if fieldCount > len(index.Fields) {
		fieldCount = len(index.Fields)
	}
	return buildKey(tuple, index.Fields, index.Prefixes, fieldCount, 0, false)
}

func (store *Store) secondaryKeyForInsert(index *SecondaryIndex, tuple *data.Tuple, rowID uint64) []byte {
	if store == nil || index == nil || tuple == nil {
		return nil
	}
	includeRowID := !index.Unique
	return buildKey(tuple, index.Fields, index.Prefixes, len(index.Fields), rowID, includeRowID)
}

func (store *Store) secondaryDuplicate(index *SecondaryIndex, key []byte, rowID uint64) bool {
	if store == nil || index == nil || len(key) == 0 || index.Tree == nil {
		return false
	}
	val, ok := index.Tree.Search(key)
	if !ok {
		return false
	}
	id, ok := DecodeRowID(val)
	if !ok {
		return true
	}
	return id != rowID
}

func (store *Store) hasSecondaryDuplicate(row *data.Tuple, rowID uint64) bool {
	if store == nil || store.SecondaryIndexes == nil {
		return false
	}
	for _, idx := range store.SecondaryIndexes {
		if idx == nil || !idx.Unique {
			continue
		}
		key := store.secondaryKeyForInsert(idx, row, rowID)
		if len(key) == 0 {
			continue
		}
		if store.secondaryDuplicate(idx, key, rowID) {
			return true
		}
	}
	return false
}

func (store *Store) hasSecondaryDuplicateExcept(row *data.Tuple, _ *data.Tuple, rowID uint64) bool {
	return store.hasSecondaryDuplicate(row, rowID)
}

func (store *Store) insertSecondaryIndexes(row *data.Tuple, rowID uint64) {
	if store == nil || store.SecondaryIndexes == nil {
		return
	}
	for _, idx := range store.SecondaryIndexes {
		if idx == nil || idx.Tree == nil {
			continue
		}
		key := store.secondaryKeyForInsert(idx, row, rowID)
		if len(key) == 0 {
			continue
		}
		idx.Tree.Insert(key, encodeRowID(rowID))
	}
}

func (store *Store) deleteSecondaryIndexes(row *data.Tuple, rowID uint64) {
	if store == nil || store.SecondaryIndexes == nil {
		return
	}
	for _, idx := range store.SecondaryIndexes {
		if idx == nil || idx.Tree == nil {
			continue
		}
		key := store.secondaryKeyForInsert(idx, row, rowID)
		if len(key) == 0 {
			continue
		}
		idx.Tree.Delete(key)
	}
}

func (store *Store) updateSecondaryIndexes(oldRow, newRow *data.Tuple, rowID uint64) error {
	if store == nil || store.SecondaryIndexes == nil {
		return nil
	}
	type update struct {
		idx    *SecondaryIndex
		oldKey []byte
		newKey []byte
	}
	updates := make([]update, 0, len(store.SecondaryIndexes))
	for _, idx := range store.SecondaryIndexes {
		if idx == nil || idx.Tree == nil {
			continue
		}
		oldKey := store.secondaryKeyForInsert(idx, oldRow, rowID)
		newKey := store.secondaryKeyForInsert(idx, newRow, rowID)
		if bytes.Equal(oldKey, newKey) {
			continue
		}
		if idx.Unique && store.secondaryDuplicate(idx, newKey, rowID) {
			return ErrDuplicateKey
		}
		updates = append(updates, update{idx: idx, oldKey: oldKey, newKey: newKey})
	}
	for _, upd := range updates {
		if len(upd.oldKey) != 0 {
			upd.idx.Tree.Delete(upd.oldKey)
		}
		if len(upd.newKey) != 0 {
			upd.idx.Tree.Insert(upd.newKey, encodeRowID(rowID))
		}
	}
	return nil
}
