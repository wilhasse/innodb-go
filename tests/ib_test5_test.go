package tests

import (
	"math/rand"
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	test5DB         = "test5_db"
	test5Table      = "t"
	test5Tables     = 2
	test5TrxRounds  = 2
	test5InsertRows = 20
	test5BlobMax    = 256
	test5VcharMax   = 128
)

func TestTest5Harness(t *testing.T) {
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
	if err := api.DatabaseCreate(test5DB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}

	rng := rand.New(rand.NewSource(2))
	for i := 0; i < test5Tables; i++ {
		if err := createTest5Table(test5TableName()); err != api.DB_SUCCESS {
			t.Fatalf("create table: %v", err)
		}
		for j := 0; j < test5TrxRounds; j++ {
			trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
			if trx == nil {
				t.Fatalf("TrxBegin returned nil")
			}
			var crsr *api.Cursor
			if err := api.CursorOpenTable(test5TableName(), trx, &crsr); err != api.DB_SUCCESS {
				t.Fatalf("CursorOpenTable: %v", err)
			}
			if err := api.CursorLock(crsr, api.LockIX); err != api.DB_SUCCESS {
				t.Fatalf("CursorLock: %v", err)
			}
			if err := insertTest5Rows(crsr, rng); err != api.DB_SUCCESS {
				t.Fatalf("insert rows: %v", err)
			}
			if err := updateTest5Rows(crsr, rng); err != api.DB_SUCCESS {
				t.Fatalf("update rows: %v", err)
			}
			if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
				t.Fatalf("CursorClose: %v", err)
			}
			if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
				t.Fatalf("TrxCommit: %v", err)
			}
		}
		if err := api.TableDrop(nil, test5TableName()); err != api.DB_SUCCESS {
			t.Fatalf("TableDrop: %v", err)
		}
	}
}

func createTest5Table(tableName string) api.ErrCode {
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
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_VARCHAR, api.IB_COL_NONE, 0, test5VcharMax); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c2", api.IB_BLOB, api.IB_COL_NONE, 0, 0); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c3", api.IB_INT, api.IB_COL_NONE, 0, 4); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	var idx *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "PRIMARY", &idx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "c1", 0); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.IndexSchemaSetClustered(idx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.IndexSchemaSetUnique(idx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableSchemaAddIndex(schema, "c3", &idx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "c3", 0); err != api.DB_SUCCESS {
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

func insertTest5Rows(crsr *api.Cursor, rng *rand.Rand) api.ErrCode {
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	for i := 0; i < test5InsertRows; i++ {
		vchar := randText(rng, test5VcharMax)
		blob := randText(rng, test5BlobMax)
		if err := api.ColSetValue(tpl, 0, vchar, len(vchar)); err != api.DB_SUCCESS {
			return err
		}
		if err := api.ColSetValue(tpl, 1, blob, len(blob)); err != api.DB_SUCCESS {
			return err
		}
		if err := api.TupleWriteI32(tpl, 2, int32(rng.Intn(10))); err != api.DB_SUCCESS {
			return err
		}
		err := api.CursorInsertRow(crsr, tpl)
		if err != api.DB_SUCCESS && err != api.DB_DUPLICATE_KEY {
			return err
		}
		tpl = api.TupleClear(tpl)
	}
	return api.DB_SUCCESS
}

func updateTest5Rows(crsr *api.Cursor, rng *rand.Rand) api.ErrCode {
	var idxCursor *api.Cursor
	if err := api.CursorOpenIndexUsingName(crsr, "c3", &idxCursor); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CursorSetLockMode(idxCursor, api.LockIX); err != api.DB_SUCCESS {
		return err
	}
	api.CursorSetClusterAccess(idxCursor)

	target := int32(rng.Intn(10))
	if err := api.CursorFirst(idxCursor); err != api.DB_SUCCESS {
		if err == api.DB_RECORD_NOT_FOUND {
			_ = api.CursorClose(idxCursor)
			return api.DB_SUCCESS
		}
		_ = api.CursorClose(idxCursor)
		return err
	}

	oldTpl := api.ClustReadTupleCreate(idxCursor)
	if oldTpl == nil {
		_ = api.CursorClose(idxCursor)
		return api.DB_ERROR
	}
	newTpl := api.ClustReadTupleCreate(idxCursor)
	if newTpl == nil {
		api.TupleDelete(oldTpl)
		_ = api.CursorClose(idxCursor)
		return api.DB_ERROR
	}
	defer api.TupleDelete(oldTpl)
	defer api.TupleDelete(newTpl)

	for {
		err := api.CursorReadRow(idxCursor, oldTpl)
		if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
			break
		}
		if err != api.DB_SUCCESS {
			_ = api.CursorClose(idxCursor)
			return err
		}
		var c3 int32
		if err := api.TupleReadI32(oldTpl, 2, &c3); err != api.DB_SUCCESS {
			_ = api.CursorClose(idxCursor)
			return err
		}
		if c3 == target {
			if err := api.TupleCopy(newTpl, oldTpl); err != api.DB_SUCCESS {
				_ = api.CursorClose(idxCursor)
				return err
			}
			vchar := randText(rng, test5VcharMax)
			blob := randText(rng, test5BlobMax)
			if err := api.ColSetValue(newTpl, 0, vchar, len(vchar)); err != api.DB_SUCCESS {
				_ = api.CursorClose(idxCursor)
				return err
			}
			if err := api.ColSetValue(newTpl, 1, blob, len(blob)); err != api.DB_SUCCESS {
				_ = api.CursorClose(idxCursor)
				return err
			}
			c3 = (c3 + 1) % 10
			if err := api.TupleWriteI32(newTpl, 2, c3); err != api.DB_SUCCESS {
				_ = api.CursorClose(idxCursor)
				return err
			}
			if err := api.CursorUpdateRow(idxCursor, oldTpl, newTpl); err != api.DB_SUCCESS && err != api.DB_DUPLICATE_KEY {
				_ = api.CursorClose(idxCursor)
				return err
			}
			break
		}
		if err := api.CursorNext(idxCursor); err != api.DB_SUCCESS {
			if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
				break
			}
			_ = api.CursorClose(idxCursor)
			return err
		}
		oldTpl = api.TupleClear(oldTpl)
		newTpl = api.TupleClear(newTpl)
	}

	return api.CursorClose(idxCursor)
}

func test5TableName() string {
	return test5DB + "/" + test5Table
}
