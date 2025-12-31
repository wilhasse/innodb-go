package row

import "github.com/wilhasse/innodb-go/data"

// SelectAll returns all rows.
func (store *Store) SelectAll() []*data.Tuple {
	if store == nil {
		return nil
	}
	return append([]*data.Tuple(nil), store.Rows...)
}

// SelectByKey finds the first row with the given primary key.
func (store *Store) SelectByKey(key data.Field) *data.Tuple {
	if store == nil || store.PrimaryKey < 0 {
		return nil
	}
	for _, row := range store.Rows {
		if row == nil || store.PrimaryKey >= len(row.Fields) {
			continue
		}
		if fieldsEqualPrefix(key, row.Fields[store.PrimaryKey], store.PrimaryKeyPrefix) {
			return row
		}
	}
	return nil
}

// SelectWhere filters rows by a predicate.
func (store *Store) SelectWhere(fn func(*data.Tuple) bool) []*data.Tuple {
	if store == nil || fn == nil {
		return nil
	}
	var out []*data.Tuple
	for _, row := range store.Rows {
		if fn(row) {
			out = append(out, row)
		}
	}
	return out
}
