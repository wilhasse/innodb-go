package tests

import (
	"math/rand"
	"testing"
	"time"

	"github.com/wilhasse/innodb-go/api"
)

const (
	mysqlBulkDB        = "bulk_test_mysql"
	mysqlBulkTable     = "massive_data"
	mysqlBulkRows      = 200
	mysqlBulkBatch     = 50
	mysqlBulkUserCount = 1000
)

func TestMySQLBulkInsertHarness(t *testing.T) {
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
	if err := api.DatabaseCreate(mysqlBulkDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	if err := createMySQLBulkTable(mysqlBulkTableName()); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}

	rng := rand.New(rand.NewSource(1))
	id := uint64(1)
	for start := 0; start < mysqlBulkRows; start += mysqlBulkBatch {
		end := start + mysqlBulkBatch
		if end > mysqlBulkRows {
			end = mysqlBulkRows
		}
		trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
		if trx == nil {
			t.Fatalf("TrxBegin returned nil")
		}
		var crsr *api.Cursor
		if err := api.CursorOpenTable(mysqlBulkTableName(), trx, &crsr); err != api.DB_SUCCESS {
			t.Fatalf("CursorOpenTable: %v", err)
		}
		if err := api.CursorLock(crsr, api.LockIX); err != api.DB_SUCCESS {
			t.Fatalf("CursorLock: %v", err)
		}
		if err := insertMySQLBulkRows(crsr, rng, &id, end-start); err != api.DB_SUCCESS {
			t.Fatalf("insert rows: %v", err)
		}
		if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
			t.Fatalf("CursorClose: %v", err)
		}
		if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
			t.Fatalf("TrxCommit: %v", err)
		}
	}

	var crsr *api.Cursor
	if err := api.CursorOpenTable(mysqlBulkTableName(), nil, &crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorOpenTable: %v", err)
	}
	count, err := countCursorRows(crsr)
	if err != api.DB_SUCCESS {
		t.Fatalf("count rows: %v", err)
	}
	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorClose: %v", err)
	}
	if count != mysqlBulkRows {
		t.Fatalf("rows=%d, want %d", count, mysqlBulkRows)
	}

	if err := api.TableDrop(nil, mysqlBulkTableName()); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(mysqlBulkDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createMySQLBulkTable(tableName string) api.ErrCode {
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if trx == nil {
		return api.DB_ERROR
	}
	if err := api.SchemaLockExclusive(trx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableSchemaAddCol(schema, "id", api.IB_INT, api.IB_COL_UNSIGNED|api.IB_COL_NOT_NULL, 0, 8); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableSchemaAddCol(schema, "user_id", api.IB_INT, api.IB_COL_UNSIGNED|api.IB_COL_NOT_NULL, 0, 4); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableSchemaAddCol(schema, "name", api.IB_VARCHAR, api.IB_COL_NOT_NULL, 0, 100); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableSchemaAddCol(schema, "email", api.IB_VARCHAR, api.IB_COL_NOT_NULL, 0, 255); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableSchemaAddCol(schema, "score", api.IB_DOUBLE, api.IB_COL_NOT_NULL, 0, 8); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableSchemaAddCol(schema, "created_at", api.IB_INT, api.IB_COL_UNSIGNED|api.IB_COL_NOT_NULL, 0, 4); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableSchemaAddCol(schema, "data_blob", api.IB_BLOB, api.IB_COL_NOT_NULL, 0, 0); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	var idx *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "PRIMARY", &idx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "id", 0); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.IndexSchemaSetClustered(idx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableSchemaAddIndex(schema, "idx_user_id", &idx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "user_id", 0); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	err := api.TableCreate(trx, schema, nil)
	api.TableSchemaDelete(schema)
	if err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	return api.TrxCommit(trx)
}

func insertMySQLBulkRows(crsr *api.Cursor, rng *rand.Rand, id *uint64, count int) api.ErrCode {
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	now := uint32(time.Now().Unix())
	for i := 0; i < count; i++ {
		name := randomString(rng, 10, 50)
		email := randomEmail(rng, 8, 12)
		blob := randomString(rng, 100, 500)
		userID := uint32(rng.Intn(mysqlBulkUserCount) + 1)
		score := rng.Float64() * 100

		if err := api.TupleWriteU64(tpl, 0, *id); err != api.DB_SUCCESS {
			return err
		}
		if err := api.TupleWriteU32(tpl, 1, userID); err != api.DB_SUCCESS {
			return err
		}
		if err := api.ColSetValue(tpl, 2, []byte(name), len(name)); err != api.DB_SUCCESS {
			return err
		}
		if err := api.ColSetValue(tpl, 3, []byte(email), len(email)); err != api.DB_SUCCESS {
			return err
		}
		if err := api.TupleWriteDouble(tpl, 4, score); err != api.DB_SUCCESS {
			return err
		}
		if err := api.TupleWriteU32(tpl, 5, now); err != api.DB_SUCCESS {
			return err
		}
		if err := api.ColSetValue(tpl, 6, []byte(blob), len(blob)); err != api.DB_SUCCESS {
			return err
		}
		if err := api.CursorInsertRow(crsr, tpl); err != api.DB_SUCCESS {
			return err
		}
		*id = *id + 1
		tpl = api.TupleClear(tpl)
	}
	return api.DB_SUCCESS
}

func mysqlBulkTableName() string {
	return mysqlBulkDB + "/" + mysqlBulkTable
}
