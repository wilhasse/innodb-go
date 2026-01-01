package srv

import (
	"testing"

	"github.com/wilhasse/innodb-go/buf"
	"github.com/wilhasse/innodb-go/log"
)

func TestAdaptiveFlushAdvancesCheckpoint(t *testing.T) {
	log.Init()
	defer log.Shutdown()

	pool := buf.NewPool(2, buf.BufPoolDefaultPageSize)
	oldPools := buf.DefaultPools()
	buf.SetDefaultPools([]*buf.Pool{pool})
	defer buf.SetDefaultPools(oldPools)

	page, _, err := pool.Fetch(1, 1)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	page.Data[0] = 0xAB
	pool.MarkDirty(page)
	pool.Release(page)

	end, _ := log.ReserveAndWriteFast([]byte("abc"))
	log.FlushUpTo(end)

	if got := log.CheckpointLSN(); got != 0 {
		t.Fatalf("expected checkpoint 0, got %d", got)
	}
	if flushed := AdaptiveFlush(0); flushed == 0 {
		t.Fatalf("expected pages flushed")
	}
	if log.CheckpointLSN() != log.FlushedLSN() {
		t.Fatalf("checkpoint=%d flushed=%d", log.CheckpointLSN(), log.FlushedLSN())
	}
	if stats := pool.Stats(); stats.Dirty != 0 {
		t.Fatalf("expected dirty 0, got %d", stats.Dirty)
	}
}
