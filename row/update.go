package row

import (
	"errors"

	"github.com/wilhasse/innodb-go/data"
)

// ErrRowNotFound indicates an update target was not found.
var ErrRowNotFound = errors.New("row: row not found")

// UpdateByKey updates a row identified by the primary key.
func (store *Store) UpdateByKey(key data.Field, updates map[int]data.Field) (*data.Tuple, error) {
	if store == nil {
		return nil, errors.New("row: nil store")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	var row *data.Tuple
	for _, candidate := range store.Rows {
		if candidate == nil || store.PrimaryKey < 0 || store.PrimaryKey >= len(candidate.Fields) {
			continue
		}
		if fieldsEqualPrefix(key, candidate.Fields[store.PrimaryKey], store.PrimaryKeyPrefix) {
			row = candidate
			break
		}
	}
	if row == nil {
		return nil, ErrRowNotFound
	}
	applyUpdates(row, updates)
	return row, nil
}

// UpdateWhere updates rows matching a predicate and returns count.
func (store *Store) UpdateWhere(fn func(*data.Tuple) bool, updates map[int]data.Field) int {
	if store == nil || fn == nil {
		return 0
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	updated := 0
	for _, row := range store.Rows {
		if fn(row) {
			applyUpdates(row, updates)
			updated++
		}
	}
	return updated
}

func applyUpdates(row *data.Tuple, updates map[int]data.Field) {
	if row == nil {
		return
	}
	for idx, field := range updates {
		if idx < 0 || idx >= len(row.Fields) {
			continue
		}
		row.Fields[idx] = field
	}
}
