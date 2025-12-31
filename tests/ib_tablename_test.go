package tests

import (
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

func TestTableNameHarness(t *testing.T) {
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

	invalid := []string{
		"",
		"a",
		"ab",
		".",
		"./",
		"../",
		"/",
		"/aaaaa",
		"/a/a",
		"abcdef/",
	}

	for _, name := range invalid {
		var schema *api.TableSchema
		if err := api.TableSchemaCreate(name, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_DATA_MISMATCH {
			t.Fatalf("TableSchemaCreate(%q)=%v, want DB_DATA_MISMATCH", name, err)
		}
	}

	var schema *api.TableSchema
	if err := api.TableSchemaCreate("a/b", &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		t.Fatalf("TableSchemaCreate valid: %v", err)
	}
	api.TableSchemaDelete(schema)
}
