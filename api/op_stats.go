package api

import (
	"sync"
	"time"
)

// OpType identifies a performance operation.
type OpType int

const (
	OpInsert OpType = iota
	OpCopy
	OpJoin
)

// OpTime tracks elapsed durations.
type OpTime struct {
	Times []time.Duration
}

// OpTimeStats summarizes timing metrics.
type OpTimeStats struct {
	Count int
	Total time.Duration
	Avg   time.Duration
	Min   time.Duration
	Max   time.Duration
}

// OpStatsSnapshot captures aggregated stats for operations.
type OpStatsSnapshot struct {
	Insert OpTimeStats
	Copy   OpTimeStats
	Join   OpTimeStats
}

var (
	opStatsMu      sync.Mutex
	opStatsEnabled bool
	opStats        = struct {
		Insert OpTime
		Copy   OpTime
		Join   OpTime
	}{}
)

// StatsEnable toggles collection of operation stats.
func StatsEnable(enabled bool) {
	opStatsMu.Lock()
	opStatsEnabled = enabled
	opStatsMu.Unlock()
}

// StatsReset clears recorded stats.
func StatsReset() {
	opStatsMu.Lock()
	opStats.Insert.Times = nil
	opStats.Copy.Times = nil
	opStats.Join.Times = nil
	opStatsMu.Unlock()
}

// StatsCollect records an elapsed duration for an operation.
func StatsCollect(op OpType, elapsed time.Duration) {
	opStatsMu.Lock()
	if !opStatsEnabled {
		opStatsMu.Unlock()
		return
	}
	switch op {
	case OpInsert:
		opStats.Insert.Times = append(opStats.Insert.Times, elapsed)
	case OpCopy:
		opStats.Copy.Times = append(opStats.Copy.Times, elapsed)
	case OpJoin:
		opStats.Join.Times = append(opStats.Join.Times, elapsed)
	}
	opStatsMu.Unlock()
}

// StatsSnapshot returns aggregated stats for each operation.
func StatsSnapshot() OpStatsSnapshot {
	opStatsMu.Lock()
	defer opStatsMu.Unlock()
	return OpStatsSnapshot{
		Insert: opStats.Insert.stats(),
		Copy:   opStats.Copy.stats(),
		Join:   opStats.Join.stats(),
	}
}

func (op *OpTime) stats() OpTimeStats {
	stats := OpTimeStats{Count: len(op.Times)}
	if stats.Count == 0 {
		return stats
	}
	min := op.Times[0]
	max := op.Times[0]
	var total time.Duration
	for _, t := range op.Times {
		total += t
		if t < min {
			min = t
		}
		if t > max {
			max = t
		}
	}
	stats.Total = total
	stats.Avg = total / time.Duration(stats.Count)
	stats.Min = min
	stats.Max = max
	return stats
}
