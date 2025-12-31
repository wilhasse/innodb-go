package row

import (
	"testing"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/dict"
	"github.com/wilhasse/innodb-go/trx"
)

func TestPrebuiltLifecycle(t *testing.T) {
	table := &dict.Table{Name: "t"}
	pre := NewPrebuilt(table, 2)
	if pre.Magic != PrebuiltAllocated || pre.Magic2 != PrebuiltAllocated {
		t.Fatalf("unexpected magic")
	}
	if !pre.SQLStatStart {
		t.Fatalf("expected SQLStatStart")
	}
	pre.RowCache.Add(&data.Tuple{})
	if len(pre.RowCache.Rows) != 1 {
		t.Fatalf("cache rows=%d", len(pre.RowCache.Rows))
	}
	if err := pre.Reset(); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if len(pre.RowCache.Rows) != 0 {
		t.Fatalf("expected cache cleared")
	}
	if err := pre.UpdateTrx(&trx.Trx{}); err != nil {
		t.Fatalf("update trx: %v", err)
	}
	if pre.Trx == nil {
		t.Fatalf("expected trx set")
	}
	if err := pre.Free(); err != nil {
		t.Fatalf("free: %v", err)
	}
	if pre.Magic != PrebuiltFreed || pre.Table != nil {
		t.Fatalf("expected freed")
	}
	if err := pre.Reset(); err == nil {
		t.Fatalf("expected reset failure after free")
	}
}
