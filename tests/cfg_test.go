package tests

import (
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

func TestCfgHarness(t *testing.T) {
	resetAPI(t)
	if err := api.Init(); err != api.DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	defer func() {
		_ = api.Shutdown(api.ShutdownNormal)
	}()

	names, err := api.CfgGetAll()
	if err != api.DB_SUCCESS || len(names) == 0 {
		t.Fatalf("CfgGetAll: %v names=%d", err, len(names))
	}
	for _, name := range names {
		typ, err := api.CfgVarGetType(name)
		if err != api.DB_SUCCESS {
			t.Fatalf("CfgVarGetType(%s): %v", name, err)
		}
		switch typ {
		case api.CfgTypeBool:
			var v bool
			if err := api.CfgGet(name, &v); err != api.DB_SUCCESS {
				t.Fatalf("CfgGet(%s): %v", name, err)
			}
		case api.CfgTypeUlint:
			var v api.Ulint
			if err := api.CfgGet(name, &v); err != api.DB_SUCCESS {
				t.Fatalf("CfgGet(%s): %v", name, err)
			}
		case api.CfgTypeUlong:
			var v uint64
			if err := api.CfgGet(name, &v); err != api.DB_SUCCESS {
				t.Fatalf("CfgGet(%s): %v", name, err)
			}
		case api.CfgTypeText:
			var v string
			if err := api.CfgGet(name, &v); err != api.DB_SUCCESS {
				t.Fatalf("CfgGet(%s): %v", name, err)
			}
		case api.CfgTypeCallback:
			var v api.Callback
			if err := api.CfgGet(name, &v); err != api.DB_SUCCESS {
				t.Fatalf("CfgGet(%s): %v", name, err)
			}
		}
	}

	if err := api.CfgSet("data_home_dir", "/some/path"); err != api.DB_INVALID_INPUT {
		t.Fatalf("expected invalid input for data_home_dir, got %v", err)
	}
	if err := api.CfgSet("data_home_dir", "/some/path/"); err != api.DB_SUCCESS {
		t.Fatalf("expected success for data_home_dir, got %v", err)
	}
	var dataHome string
	if err := api.CfgGet("data_home_dir", &dataHome); err != api.DB_SUCCESS || dataHome != "/some/path/" {
		t.Fatalf("data_home_dir=%q err=%v", dataHome, err)
	}

	if err := api.CfgSet("buffer_pool_size", uint64(0xFFFFFFFF-5)); err != api.DB_SUCCESS {
		t.Fatalf("buffer_pool_size: %v", err)
	}
	if err := api.CfgSet("flush_method", "fdatasync"); err != api.DB_INVALID_INPUT {
		t.Fatalf("expected invalid flush_method, got %v", err)
	}

	for i := 0; i <= 100; i++ {
		err := api.CfgSet("lru_old_blocks_pct", i)
		if i >= 5 && i <= 95 {
			if err != api.DB_SUCCESS {
				t.Fatalf("lru_old_blocks_pct=%d err=%v", i, err)
			}
			var val api.Ulint
			if err := api.CfgGet("lru_old_blocks_pct", &val); err != api.DB_SUCCESS || int(val) != i {
				t.Fatalf("lru_old_blocks_pct get=%d err=%v", val, err)
			}
		} else if err != api.DB_INVALID_INPUT {
			t.Fatalf("expected invalid input for lru_old_blocks_pct=%d, got %v", i, err)
		}
	}

	if err := api.CfgSet("lru_block_access_recency", 123); err != api.DB_SUCCESS {
		t.Fatalf("lru_block_access_recency: %v", err)
	}
	if err := api.CfgSet("open_files", 123); err != api.DB_SUCCESS {
		t.Fatalf("open_files: %v", err)
	}
}

func resetAPI(t *testing.T) {
	t.Helper()
	t.Setenv("INNODB_DATA_HOME_DIR", t.TempDir())
	_ = api.Shutdown(api.ShutdownNormal)
}
