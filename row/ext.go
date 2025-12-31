package row

import "github.com/wilhasse/innodb-go/data"

// MaxIndexColLen is the default prefix size for external columns.
const MaxIndexColLen = 256

// ExtCache stores cached prefixes for externally stored columns.
type ExtCache struct {
	ExtCols  []int
	Prefixes [][]byte
	Lengths  []uint32
}

// NewExtCache creates a cache for external column prefixes.
func NewExtCache(extCols []int, tuple *data.Tuple) *ExtCache {
	return NewExtCacheWithLimit(extCols, tuple, MaxIndexColLen)
}

// NewExtCacheWithLimit creates a cache with a custom prefix limit.
func NewExtCacheWithLimit(extCols []int, tuple *data.Tuple, limit int) *ExtCache {
	cache := &ExtCache{
		ExtCols:  append([]int(nil), extCols...),
		Prefixes: make([][]byte, len(extCols)),
		Lengths:  make([]uint32, len(extCols)),
	}
	for i, col := range extCols {
		cache.fillPrefix(i, col, tuple, limit)
	}
	return cache
}

// Prefix returns a cached prefix by column index.
func (cache *ExtCache) Prefix(col int) []byte {
	if cache == nil {
		return nil
	}
	for i, c := range cache.ExtCols {
		if c == col {
			return cache.Prefixes[i]
		}
	}
	return nil
}

func (cache *ExtCache) fillPrefix(i, col int, tuple *data.Tuple, limit int) {
	if cache == nil || tuple == nil || col < 0 || col >= len(tuple.Fields) {
		return
	}
	field := tuple.Fields[col]
	if field.Len == data.UnivSQLNull || !field.Ext || len(field.Data) == 0 {
		return
	}
	length := int(field.Len)
	if length > len(field.Data) {
		length = len(field.Data)
	}
	if limit > 0 && length > limit {
		length = limit
	}
	if length <= 0 {
		return
	}
	prefix := make([]byte, length)
	copy(prefix, field.Data[:length])
	cache.Prefixes[i] = prefix
	cache.Lengths[i] = uint32(length)
}
