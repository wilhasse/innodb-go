package row

import (
	"bytes"
	"testing"

	"github.com/wilhasse/innodb-go/data"
)

func TestExtCache(t *testing.T) {
	tuple := &data.Tuple{
		Fields: []data.Field{
			{Data: []byte("short"), Len: 5, Ext: true},
			{Data: []byte("internal"), Len: 8, Ext: false},
			{Data: bytes.Repeat([]byte("a"), 20), Len: 20, Ext: true},
		},
	}
	cache := NewExtCacheWithLimit([]int{0, 1, 2}, tuple, 10)
	if cache.Lengths[0] != 5 || string(cache.Prefixes[0]) != "short" {
		t.Fatalf("prefix0=%q len=%d", cache.Prefixes[0], cache.Lengths[0])
	}
	if cache.Lengths[1] != 0 {
		t.Fatalf("expected non-external length 0, got %d", cache.Lengths[1])
	}
	if cache.Lengths[2] != 10 || len(cache.Prefixes[2]) != 10 {
		t.Fatalf("prefix2 len=%d", cache.Lengths[2])
	}
	if len(cache.Prefix(2)) != 10 {
		t.Fatalf("prefix lookup len=%d", len(cache.Prefix(2)))
	}
}
