package row

import (
	"bytes"
	"errors"

	"github.com/wilhasse/innodb-go/data"
)

// MergeStores merges two row stores with the same primary key.
func MergeStores(left, right *Store) (*Store, error) {
	if left == nil || right == nil {
		return nil, errors.New("row: nil store")
	}
	if left.PrimaryKey != right.PrimaryKey {
		return nil, errors.New("row: mismatched primary key")
	}
	rows, err := MergeTuples(left.Rows, right.Rows, left.PrimaryKey)
	if err != nil {
		return nil, err
	}
	merged := &Store{
		Rows:               rows,
		PrimaryKey:         left.PrimaryKey,
		PrimaryKeyPrefix:   left.PrimaryKeyPrefix,
		PrimaryKeyFields:   append([]int(nil), left.PrimaryKeyFields...),
		PrimaryKeyPrefixes: append([]int(nil), left.PrimaryKeyPrefixes...),
	}
	merged.rebuildIndex()
	return merged, nil
}

// MergeTuples merges two sorted tuple slices by key field.
func MergeTuples(left, right []*data.Tuple, keyField int) ([]*data.Tuple, error) {
	if keyField < 0 {
		out := append([]*data.Tuple(nil), left...)
		out = append(out, right...)
		return out, nil
	}
	var merged []*data.Tuple
	i, j := 0, 0
	for i < len(left) && j < len(right) {
		lkey, lok := keyAt(left[i], keyField)
		rkey, rok := keyAt(right[j], keyField)
		if !lok || !rok {
			return nil, errors.New("row: missing key field")
		}
		switch cmp := compareField(lkey, rkey); {
		case cmp == 0:
			return nil, ErrDuplicateKey
		case cmp < 0:
			merged = append(merged, left[i])
			i++
		default:
			merged = append(merged, right[j])
			j++
		}
	}
	merged = append(merged, left[i:]...)
	merged = append(merged, right[j:]...)
	return merged, nil
}

func keyAt(tuple *data.Tuple, keyField int) (data.Field, bool) {
	if tuple == nil || keyField < 0 || keyField >= len(tuple.Fields) {
		return data.Field{}, false
	}
	return tuple.Fields[keyField], true
}

func compareField(a, b data.Field) int {
	if a.Len == data.UnivSQLNull && b.Len == data.UnivSQLNull {
		return 0
	}
	if a.Len == data.UnivSQLNull {
		return 1
	}
	if b.Len == data.UnivSQLNull {
		return -1
	}
	return bytes.Compare(a.Data, b.Data)
}
