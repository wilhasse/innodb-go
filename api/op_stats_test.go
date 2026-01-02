package api

import (
	"testing"
	"time"
)

func TestOpStatsCollect(t *testing.T) {
	StatsEnable(true)
	StatsReset()

	StatsCollect(OpInsert, 10*time.Millisecond)
	StatsCollect(OpInsert, 20*time.Millisecond)
	StatsCollect(OpCopy, 5*time.Millisecond)

	stats := StatsSnapshot()
	if stats.Insert.Count != 2 {
		t.Fatalf("insert count=%d", stats.Insert.Count)
	}
	if stats.Copy.Count != 1 {
		t.Fatalf("copy count=%d", stats.Copy.Count)
	}
	if stats.Join.Count != 0 {
		t.Fatalf("join count=%d", stats.Join.Count)
	}
	if stats.Insert.Total != 30*time.Millisecond {
		t.Fatalf("insert total=%v", stats.Insert.Total)
	}
}

func TestOpStatsDisabled(t *testing.T) {
	StatsEnable(false)
	StatsReset()
	StatsCollect(OpJoin, 3*time.Millisecond)
	stats := StatsSnapshot()
	if stats.Join.Count != 0 {
		t.Fatalf("expected no stats when disabled")
	}
}
