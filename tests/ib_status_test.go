package tests

import (
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

func TestStatusHarness(t *testing.T) {
	resetAPI(t)
	if err := api.Init(); err != api.DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	defer func() {
		_ = api.Shutdown(api.ShutdownNormal)
	}()

	if err := api.Startup("barracuda"); err != api.DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}

	names := []string{
		"read_req_pending",
		"write_req_pending",
		"fsync_req_pending",
		"write_req_done",
		"read_req_done",
		"fsync_req_done",
		"bytes_total_written",
		"bytes_total_read",
		"buffer_pool_current_size",
		"buffer_pool_data_pages",
		"buffer_pool_dirty_pages",
		"buffer_pool_misc_pages",
		"buffer_pool_free_pages",
		"buffer_pool_read_reqs",
		"buffer_pool_reads",
		"buffer_pool_waited_for_free",
		"buffer_pool_pages_flushed",
		"buffer_pool_write_reqs",
		"buffer_pool_total_pages",
		"buffer_pool_pages_read",
		"buffer_pool_pages_written",
		"double_write_pages_written",
		"double_write_invoked",
		"log_buffer_slot_waits",
		"log_write_reqs",
		"log_write_flush_count",
		"log_bytes_written",
		"log_fsync_req_done",
		"log_write_req_pending",
		"log_fsync_req_pending",
		"lock_row_waits",
		"lock_row_waiting",
		"lock_total_wait_time_in_secs",
		"lock_wait_time_avg_in_secs",
		"lock_max_wait_time_in_secs",
		"row_total_read",
		"row_total_inserted",
		"row_total_updated",
		"row_total_deleted",
		"page_size",
		"have_atomic_builtins",
	}

	for _, name := range names {
		var val int64
		if err := api.StatusGetI64(name, &val); err != api.DB_SUCCESS {
			t.Fatalf("StatusGetI64(%s): %v", name, err)
		}
	}
}
