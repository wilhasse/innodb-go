package tests

import (
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

func TestCompressedTables(t *testing.T) {
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
	if err := api.CfgSet("file_per_table", true); err != api.DB_SUCCESS {
		t.Fatalf("CfgSet: %v", err)
	}
	if err := api.DatabaseCreate("test"); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}

	validPageSizes := []int{0, 1, 2, 4, 8, 16}
	for _, size := range validPageSizes {
		var schema *api.TableSchema
		if err := api.TableSchemaCreate("test/t_compressed", &schema, api.IB_TBL_COMPRESSED, size); err != api.DB_SUCCESS {
			t.Fatalf("TableSchemaCreate size=%d: %v", size, err)
		}
		if err := api.TableSchemaAddCol(schema, "c1", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
			t.Fatalf("TableSchemaAddCol: %v", err)
		}
		var idx *api.IndexSchema
		if err := api.TableSchemaAddIndex(schema, "PRIMARY_KEY", &idx); err != api.DB_SUCCESS {
			t.Fatalf("TableSchemaAddIndex: %v", err)
		}
		if err := api.IndexSchemaAddCol(idx, "c1", 0); err != api.DB_SUCCESS {
			t.Fatalf("IndexSchemaAddCol: %v", err)
		}
		if err := api.IndexSchemaSetClustered(idx); err != api.DB_SUCCESS {
			t.Fatalf("IndexSchemaSetClustered: %v", err)
		}
		if err := api.TableCreate(nil, schema, nil); err != api.DB_SUCCESS {
			t.Fatalf("TableCreate: %v", err)
		}
		api.TableSchemaDelete(schema)
		if err := api.TableDrop(nil, "test/t_compressed"); err != api.DB_SUCCESS {
			t.Fatalf("TableDrop: %v", err)
		}
	}

	invalidPageSizes := []int{3, 5, 6, 14, 17, 32, 128, 301}
	for _, size := range invalidPageSizes {
		var schema *api.TableSchema
		if err := api.TableSchemaCreate("test/t_compressed", &schema, api.IB_TBL_COMPRESSED, size); err != api.DB_INVALID_INPUT {
			t.Fatalf("expected invalid size %d, got %v", size, err)
		}
	}

	if err := api.DatabaseDrop("test"); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}
