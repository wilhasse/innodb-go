package rec

import (
	"bytes"

	"github.com/wilhasse/innodb-go/data"
)

// CompareRecords compares two records using index order and prefix lengths.
// order holds column indexes; prefixes holds optional prefix lengths.
func CompareRecords(a, b []data.Field, order []int, prefixes []int) int {
	if order == nil {
		n := len(a)
		if len(b) < n {
			n = len(b)
		}
		order = make([]int, n)
		for i := 0; i < n; i++ {
			order[i] = i
		}
	}
	for i, col := range order {
		prefix := 0
		if i < len(prefixes) {
			prefix = prefixes[i]
		}
		var af, bf data.Field
		if col >= 0 && col < len(a) {
			af = a[col]
		} else {
			af.Len = data.UnivSQLNull
		}
		if col >= 0 && col < len(b) {
			bf = b[col]
		} else {
			bf.Len = data.UnivSQLNull
		}
		cmp := compareFieldPrefix(&af, &bf, prefix)
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

// CompareTuples compares two tuples using index order and prefix lengths.
func CompareTuples(a, b *data.Tuple, order []int, prefixes []int) int {
	if a == nil || b == nil {
		switch {
		case a == b:
			return 0
		case a == nil:
			return -1
		default:
			return 1
		}
	}
	return CompareRecords(a.Fields, b.Fields, order, prefixes)
}

func compareFieldPrefix(a, b *data.Field, prefix int) int {
	if a == nil || b == nil {
		switch {
		case a == b:
			return 0
		case a == nil:
			return -1
		default:
			return 1
		}
	}
	if data.FieldIsNull(a) || data.FieldIsNull(b) {
		switch {
		case data.FieldIsNull(a) && data.FieldIsNull(b):
			return 0
		case data.FieldIsNull(a):
			return -1
		default:
			return 1
		}
	}
	if prefix <= 0 {
		return data.CompareFields(a, b)
	}
	alen := int(a.Len)
	blen := int(b.Len)
	if alen > len(a.Data) {
		alen = len(a.Data)
	}
	if blen > len(b.Data) {
		blen = len(b.Data)
	}
	ap := alen
	bp := blen
	if ap > prefix {
		ap = prefix
	}
	if bp > prefix {
		bp = prefix
	}
	cmp := bytes.Compare(a.Data[:ap], b.Data[:bp])
	if cmp != 0 {
		return cmp
	}
	if ap == bp {
		if alen < prefix && blen < prefix {
			if alen == blen {
				return 0
			}
			if alen < blen {
				return -1
			}
			return 1
		}
		return 0
	}
	if ap < bp {
		return -1
	}
	return 1
}
