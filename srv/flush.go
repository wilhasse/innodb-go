package srv

import (
	"github.com/wilhasse/innodb-go/buf"
	"github.com/wilhasse/innodb-go/log"
)

// AdaptiveFlush flushes dirty pages when the checkpoint lags the flushed LSN.
func AdaptiveFlush(limit int) int {
	flushed := log.FlushedLSN()
	checkpoint := log.CheckpointLSN()
	if flushed <= checkpoint {
		return 0
	}
	pools := buf.DefaultPools()
	if len(pools) == 0 {
		return 0
	}
	total := 0
	if limit <= 0 {
		for _, pool := range pools {
			if pool != nil {
				total += pool.FlushList(0)
			}
		}
	} else {
		perPool := limit / len(pools)
		if perPool < 1 {
			perPool = 1
		}
		for _, pool := range pools {
			if pool != nil {
				total += pool.FlushList(perPool)
			}
		}
	}
	if total > 0 {
		log.Checkpoint()
	}
	return total
}
