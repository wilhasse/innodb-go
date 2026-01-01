package srv

import (
	"sync/atomic"

	"github.com/wilhasse/innodb-go/buf"
	"github.com/wilhasse/innodb-go/fil"
	iblog "github.com/wilhasse/innodb-go/log"
	ibos "github.com/wilhasse/innodb-go/os"
	"github.com/wilhasse/innodb-go/ut"
)

// ExportStatusVars mirrors the C export_vars struct used for status reporting.
type ExportStatusVars struct {
	InnodbDataPendingReads        ut.Ulint
	InnodbDataPendingWrites       ut.Ulint
	InnodbDataPendingFsyncs       ut.Ulint
	InnodbDataWrites              ut.Ulint
	InnodbDataReads               ut.Ulint
	InnodbDataFsyncs              ut.Ulint
	InnodbDataWritten             ut.Ulint
	InnodbDataRead                ut.Ulint
	InnodbBufferPoolPagesTotal    ut.Ulint
	InnodbBufferPoolPagesData     ut.Ulint
	InnodbBufferPoolPagesDirty    ut.Ulint
	InnodbBufferPoolPagesMisc     ut.Ulint
	InnodbBufferPoolPagesFree     ut.Ulint
	InnodbBufferPoolReadRequests  ut.Ulint
	InnodbBufferPoolReads         ut.Ulint
	InnodbBufferPoolWaitFree      ut.Ulint
	InnodbBufferPoolPagesFlushed  ut.Ulint
	InnodbBufferPoolWriteRequests ut.Ulint
	InnodbPagesCreated            ut.Ulint
	InnodbPagesRead               ut.Ulint
	InnodbPagesWritten            ut.Ulint
	InnodbDblwrPagesWritten       ut.Ulint
	InnodbDblwrWrites             ut.Ulint
	InnodbLogWaits                ut.Ulint
	InnodbLogWriteRequests        ut.Ulint
	InnodbLogWrites               ut.Ulint
	InnodbOsLogWritten            ut.Ulint
	InnodbOsLogFsyncs             ut.Ulint
	InnodbOsLogPendingWrites      ut.Ulint
	InnodbOsLogPendingFsyncs      ut.Ulint
	InnodbRowLockWaits            ut.Ulint
	InnodbRowLockCurrentWaits     ut.Ulint
	InnodbRowLockTime             ut.Ulint
	InnodbRowLockTimeAvg          ut.Ulint
	InnodbRowLockTimeMax          ut.Ulint
	InnodbRowsRead                ut.Ulint
	InnodbRowsInserted            ut.Ulint
	InnodbRowsUpdated             ut.Ulint
	InnodbRowsDeleted             ut.Ulint
	InnodbPageSize                ut.Ulint
	InnodbHaveAtomicBuiltins      ut.IBool
}

// ExportVars holds the current status counters.
var ExportVars = ExportStatusVars{
	InnodbPageSize: ut.UNIV_PAGE_SIZE,
}

// ExportInnoDBStatus refreshes ExportVars with the latest counters.
func ExportInnoDBStatus() {
	ExportVars.InnodbPageSize = ut.UNIV_PAGE_SIZE
	ExportVars.InnodbHaveAtomicBuiltins = ut.IBool(1)

	total := 0
	used := 0
	dirty := 0
	var hits uint64
	var misses uint64
	for _, pool := range buf.DefaultPools() {
		if pool == nil {
			continue
		}
		stats := pool.Stats()
		total += stats.Capacity
		used += stats.Size
		dirty += stats.Dirty
		hits += stats.Hits
		misses += stats.Misses
	}
	if total < used {
		total = used
	}
	free := 0
	if total > used {
		free = total - used
	}
	readReqs := hits + misses

	reads := atomic.LoadUint64(&ibos.NFileReads)
	writes := atomic.LoadUint64(&ibos.NFileWrites)
	syncs := atomic.LoadUint64(&ibos.NFileSyncs)
	logFlushes := atomic.LoadUint64(&iblog.NLogFlushes)
	pendingLogFlushes := atomic.LoadUint64(&iblog.NPendingLogFlushes)
	pendingSpaceFlushes := atomic.LoadUint64(&fil.NPendingTablespaceFlushes)

	ExportVars.InnodbDataPendingWrites = ut.Ulint(pendingSpaceFlushes)
	ExportVars.InnodbDataPendingFsyncs = ut.Ulint(pendingSpaceFlushes)
	ExportVars.InnodbDataWrites = ut.Ulint(writes)
	ExportVars.InnodbDataReads = ut.Ulint(reads)
	ExportVars.InnodbDataFsyncs = ut.Ulint(syncs)
	ExportVars.InnodbDataWritten = ut.Ulint(writes) * ut.UNIV_PAGE_SIZE
	ExportVars.InnodbDataRead = ut.Ulint(reads) * ut.UNIV_PAGE_SIZE

	ExportVars.InnodbBufferPoolPagesTotal = ut.Ulint(total)
	ExportVars.InnodbBufferPoolPagesData = ut.Ulint(used)
	ExportVars.InnodbBufferPoolPagesDirty = ut.Ulint(dirty)
	ExportVars.InnodbBufferPoolPagesFree = ut.Ulint(free)
	ExportVars.InnodbBufferPoolReadRequests = ut.Ulint(readReqs)
	ExportVars.InnodbBufferPoolReads = ut.Ulint(misses)
	ExportVars.InnodbBufferPoolWaitFree = 0
	ExportVars.InnodbBufferPoolPagesFlushed = ut.Ulint(writes)
	ExportVars.InnodbBufferPoolWriteRequests = ut.Ulint(writes)

	ExportVars.InnodbPagesRead = ut.Ulint(reads)
	ExportVars.InnodbPagesWritten = ut.Ulint(writes)

	ExportVars.InnodbLogWriteRequests = ut.Ulint(logFlushes)
	ExportVars.InnodbLogWrites = ut.Ulint(logFlushes)
	ExportVars.InnodbOsLogWritten = ut.Ulint(iblog.CurrentLSN())
	ExportVars.InnodbOsLogFsyncs = ut.Ulint(syncs)
	ExportVars.InnodbOsLogPendingWrites = ut.Ulint(pendingLogFlushes)
	ExportVars.InnodbOsLogPendingFsyncs = ut.Ulint(pendingLogFlushes)
}
