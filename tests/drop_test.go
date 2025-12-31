package tests

import (
	"fmt"
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	dropDB        = "drop_test"
	dropTableName = "t"
)

func TestDropHarness(t *testing.T) {
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
	if err := api.DatabaseCreate(dropDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}

	for i := 0; i < 10; i++ {
		if err := createDropTable(i); err != api.DB_SUCCESS {
			t.Fatalf("create table %d: %v", i, err)
		}
	}

	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	for i := 0; i < 10; i++ {
		tableName := fmt.Sprintf("%s/%s%d", dropDB, dropTableName, i)
		var crsr *api.Cursor
		if err := api.CursorOpenTable(tableName, trx, &crsr); err != api.DB_SUCCESS {
			t.Fatalf("CursorOpenTable %s: %v", tableName, err)
		}
		if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
			t.Fatalf("CursorClose %s: %v", tableName, err)
		}
	}
	if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
		t.Fatalf("TrxCommit: %v", err)
	}

	if err := api.DatabaseDrop(dropDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createDropTable(n int) api.ErrCode {
	tableName := fmt.Sprintf("%s/%s%d", dropDB, dropTableName, n)
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if err := api.SchemaLockExclusive(trx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableCreate(trx, schema, nil); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	api.TableSchemaDelete(schema)
	return api.TrxCommit(trx)
}
