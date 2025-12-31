package tests

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	test2DB         = "test2_db"
	test2Table      = "ib_test2"
	test2Tables     = 3
	test2TrxRounds  = 2
	test2InsertRows = 20
	test2BlobMax    = 256
	test2VcharMax   = 128
)

func TestTest2Harness(t *testing.T) {
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
	if err := api.DatabaseCreate(test2DB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}

	rng := rand.New(rand.NewSource(1))
	keyCounter := 0

	for i := 0; i < test2Tables; i++ {
		if err := createTest2Table(test2TableName()); err != api.DB_SUCCESS {
			t.Fatalf("create table: %v", err)
		}

		keys := make([]string, 0, test2InsertRows*test2TrxRounds)
		for j := 0; j < test2TrxRounds; j++ {
			trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
			if trx == nil {
				t.Fatalf("TrxBegin returned nil")
			}
			var crsr *api.Cursor
			if err := api.CursorOpenTable(test2TableName(), trx, &crsr); err != api.DB_SUCCESS {
				t.Fatalf("CursorOpenTable: %v", err)
			}
			if err := api.CursorLock(crsr, api.LockIX); err != api.DB_SUCCESS {
				t.Fatalf("CursorLock: %v", err)
			}
			if err := insertTest2Rows(crsr, rng, &keys, &keyCounter); err != api.DB_SUCCESS {
				t.Fatalf("insert rows: %v", err)
			}
			if err := updateTest2Row(crsr, rng, &keys, &keyCounter); err != api.DB_SUCCESS {
				t.Fatalf("update row: %v", err)
			}
			if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
				t.Fatalf("CursorClose: %v", err)
			}
			if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
				t.Fatalf("TrxCommit: %v", err)
			}
		}

		if err := api.TableDrop(nil, test2TableName()); err != api.DB_SUCCESS {
			t.Fatalf("TableDrop: %v", err)
		}
	}
}

func createTest2Table(tableName string) api.ErrCode {
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
	if err := api.TableSchemaAddCol(schema, "vchar", api.IB_VARCHAR, api.IB_COL_NONE, 0, test2VcharMax); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableSchemaAddCol(schema, "blob", api.IB_BLOB, api.IB_COL_NONE, 0, 0); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.TableSchemaAddCol(schema, "count", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	var idx *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "PRIMARY", &idx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "vchar", 0); err != api.DB_SUCCESS {
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
	err := api.TableCreate(trx, schema, nil)
	api.TableSchemaDelete(schema)
	if err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	return api.TrxCommit(trx)
}

func insertTest2Rows(crsr *api.Cursor, rng *rand.Rand, keys *[]string, counter *int) api.ErrCode {
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	for i := 0; i < test2InsertRows; i++ {
		*counter = *counter + 1
		key := fmt.Sprintf("k_%06d", *counter)
		blob := randText(rng, test2BlobMax)
		if err := api.ColSetValue(tpl, 0, []byte(key), len(key)); err != api.DB_SUCCESS {
			return err
		}
		if err := api.ColSetValue(tpl, 1, blob, len(blob)); err != api.DB_SUCCESS {
			return err
		}
		if err := api.TupleWriteU32(tpl, 2, 0); err != api.DB_SUCCESS {
			return err
		}
		if err := api.CursorInsertRow(crsr, tpl); err != api.DB_SUCCESS {
			return err
		}
		*keys = append(*keys, key)
		tpl = api.TupleClear(tpl)
	}
	return api.DB_SUCCESS
}

func updateTest2Row(crsr *api.Cursor, rng *rand.Rand, keys *[]string, counter *int) api.ErrCode {
	if len(*keys) == 0 {
		return api.DB_SUCCESS
	}
	idx := rng.Intn(len(*keys))
	oldKey := (*keys)[idx]

	if err := api.CursorFirst(crsr); err != api.DB_SUCCESS {
		if err == api.DB_RECORD_NOT_FOUND {
			return api.DB_SUCCESS
		}
		return err
	}

	oldTpl := api.ClustReadTupleCreate(crsr)
	if oldTpl == nil {
		return api.DB_ERROR
	}
	newTpl := api.ClustReadTupleCreate(crsr)
	if newTpl == nil {
		api.TupleDelete(oldTpl)
		return api.DB_ERROR
	}
	defer api.TupleDelete(oldTpl)
	defer api.TupleDelete(newTpl)

	for {
		err := api.CursorReadRow(crsr, oldTpl)
		if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
			return api.DB_SUCCESS
		}
		if err != api.DB_SUCCESS {
			return err
		}
		vchar, err := tupleString(oldTpl, 0)
		if err != api.DB_SUCCESS {
			return err
		}
		if vchar == oldKey {
			if err := api.TupleCopy(newTpl, oldTpl); err != api.DB_SUCCESS {
				return err
			}
			var count uint32
			if err := api.TupleReadU32(oldTpl, 2, &count); err != api.DB_SUCCESS {
				return err
			}
			count++
			*counter = *counter + 1
			newKey := fmt.Sprintf("k_%06d", *counter)
			blob := randText(rng, test2BlobMax)
			if err := api.ColSetValue(newTpl, 0, []byte(newKey), len(newKey)); err != api.DB_SUCCESS {
				return err
			}
			if err := api.ColSetValue(newTpl, 1, blob, len(blob)); err != api.DB_SUCCESS {
				return err
			}
			if err := api.TupleWriteU32(newTpl, 2, count); err != api.DB_SUCCESS {
				return err
			}
			if err := api.CursorUpdateRow(crsr, oldTpl, newTpl); err != api.DB_SUCCESS {
				return err
			}
			(*keys)[idx] = newKey
			return api.DB_SUCCESS
		}
		if err := api.CursorNext(crsr); err != api.DB_SUCCESS {
			if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
				return api.DB_SUCCESS
			}
			return err
		}
		oldTpl = api.TupleClear(oldTpl)
		newTpl = api.TupleClear(newTpl)
	}
}

func test2TableName() string {
	return test2DB + "/" + test2Table
}
