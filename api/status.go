package api

import (
	"strings"

	"github.com/wilhasse/innodb-go/srv"
	"github.com/wilhasse/innodb-go/ut"
)

type statusType int

const (
	statusIBool statusType = iota
	statusI64
	statusUlint
)

type statusVar struct {
	name  string
	typ   statusType
	ulint *ut.Ulint
	i64   *int64
	ibool *ut.IBool
}

var statusVars = []statusVar{
	{"read_req_pending", statusUlint, &srv.ExportVars.InnodbDataPendingReads, nil, nil},
	{"write_req_pending", statusUlint, &srv.ExportVars.InnodbDataPendingWrites, nil, nil},
	{"fsync_req_pending", statusUlint, &srv.ExportVars.InnodbDataPendingFsyncs, nil, nil},
	{"write_req_done", statusUlint, &srv.ExportVars.InnodbDataWrites, nil, nil},
	{"read_req_done", statusUlint, &srv.ExportVars.InnodbDataReads, nil, nil},
	{"fsync_req_done", statusUlint, &srv.ExportVars.InnodbDataFsyncs, nil, nil},
	{"bytes_total_written", statusUlint, &srv.ExportVars.InnodbDataWritten, nil, nil},
	{"bytes_total_read", statusUlint, &srv.ExportVars.InnodbDataRead, nil, nil},
	{"buffer_pool_current_size", statusUlint, &srv.ExportVars.InnodbBufferPoolPagesTotal, nil, nil},
	{"buffer_pool_data_pages", statusUlint, &srv.ExportVars.InnodbBufferPoolPagesData, nil, nil},
	{"buffer_pool_dirty_pages", statusUlint, &srv.ExportVars.InnodbBufferPoolPagesDirty, nil, nil},
	{"buffer_pool_misc_pages", statusUlint, &srv.ExportVars.InnodbBufferPoolPagesMisc, nil, nil},
	{"buffer_pool_free_pages", statusUlint, &srv.ExportVars.InnodbBufferPoolPagesFree, nil, nil},
	{"buffer_pool_read_reqs", statusUlint, &srv.ExportVars.InnodbBufferPoolReadRequests, nil, nil},
	{"buffer_pool_reads", statusUlint, &srv.ExportVars.InnodbBufferPoolReads, nil, nil},
	{"buffer_pool_waited_for_free", statusUlint, &srv.ExportVars.InnodbBufferPoolWaitFree, nil, nil},
	{"buffer_pool_pages_flushed", statusUlint, &srv.ExportVars.InnodbBufferPoolPagesFlushed, nil, nil},
	{"buffer_pool_write_reqs", statusUlint, &srv.ExportVars.InnodbBufferPoolWriteRequests, nil, nil},
	{"buffer_pool_total_pages", statusUlint, &srv.ExportVars.InnodbPagesCreated, nil, nil},
	{"buffer_pool_pages_read", statusUlint, &srv.ExportVars.InnodbPagesRead, nil, nil},
	{"buffer_pool_pages_written", statusUlint, &srv.ExportVars.InnodbPagesWritten, nil, nil},
	{"double_write_pages_written", statusUlint, &srv.ExportVars.InnodbDblwrPagesWritten, nil, nil},
	{"double_write_invoked", statusUlint, &srv.ExportVars.InnodbDblwrWrites, nil, nil},
	{"log_buffer_slot_waits", statusUlint, &srv.ExportVars.InnodbLogWaits, nil, nil},
	{"log_write_reqs", statusUlint, &srv.ExportVars.InnodbLogWriteRequests, nil, nil},
	{"log_write_flush_count", statusUlint, &srv.ExportVars.InnodbLogWrites, nil, nil},
	{"log_bytes_written", statusUlint, &srv.ExportVars.InnodbOsLogWritten, nil, nil},
	{"log_fsync_req_done", statusUlint, &srv.ExportVars.InnodbOsLogFsyncs, nil, nil},
	{"log_write_req_pending", statusUlint, &srv.ExportVars.InnodbOsLogPendingWrites, nil, nil},
	{"log_fsync_req_pending", statusUlint, &srv.ExportVars.InnodbOsLogPendingFsyncs, nil, nil},
	{"lock_row_waits", statusUlint, &srv.ExportVars.InnodbRowLockWaits, nil, nil},
	{"lock_row_waiting", statusUlint, &srv.ExportVars.InnodbRowLockCurrentWaits, nil, nil},
	{"lock_total_wait_time_in_secs", statusUlint, &srv.ExportVars.InnodbRowLockTime, nil, nil},
	{"lock_wait_time_avg_in_secs", statusUlint, &srv.ExportVars.InnodbRowLockTimeAvg, nil, nil},
	{"lock_max_wait_time_in_secs", statusUlint, &srv.ExportVars.InnodbRowLockTimeMax, nil, nil},
	{"row_total_read", statusUlint, &srv.ExportVars.InnodbRowsRead, nil, nil},
	{"row_total_inserted", statusUlint, &srv.ExportVars.InnodbRowsInserted, nil, nil},
	{"row_total_updated", statusUlint, &srv.ExportVars.InnodbRowsUpdated, nil, nil},
	{"row_total_deleted", statusUlint, &srv.ExportVars.InnodbRowsDeleted, nil, nil},
	{"page_size", statusUlint, &srv.ExportVars.InnodbPageSize, nil, nil},
	{"have_atomic_builtins", statusIBool, nil, nil, &srv.ExportVars.InnodbHaveAtomicBuiltins},
}

// StatusGetI64 returns a status variable value as int64.
func StatusGetI64(name string, dst *int64) ErrCode {
	if dst == nil {
		return DB_INVALID_INPUT
	}
	status := lookupStatus(name)
	if status == nil {
		return DB_NOT_FOUND
	}

	srv.ExportInnoDBStatus()

	switch status.typ {
	case statusUlint:
		*dst = int64(*status.ulint)
		return DB_SUCCESS
	case statusIBool:
		*dst = int64(*status.ibool)
		return DB_SUCCESS
	case statusI64:
		*dst = *status.i64
		return DB_SUCCESS
	default:
		return DB_DATA_MISMATCH
	}
}

func lookupStatus(name string) *statusVar {
	if strings.TrimSpace(name) == "" {
		return nil
	}
	for i := range statusVars {
		if strings.EqualFold(statusVars[i].name, name) {
			return &statusVars[i]
		}
	}
	return nil
}
