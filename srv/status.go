package srv

import "github.com/wilhasse/innodb-go/ut"

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

// ExportInnoDBStatus refreshes ExportVars. Currently a stub.
func ExportInnoDBStatus() {}
