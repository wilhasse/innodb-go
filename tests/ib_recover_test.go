package tests

import (
	"math/rand"
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	recoverDB       = "recover_test"
	recoverTable    = "t"
	recoverTrxCount = 10
	recoverRows     = 100
	recoverC2MaxLen = 256
	recoverC3MaxLen = 8192
)

func TestRecoverHarness(t *testing.T) {
	resetAPI(t)
	if err := api.Init(); err != api.DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	if err := api.Startup("barracuda"); err != api.DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}

	if err := recoverPhase(); err != api.DB_SUCCESS {
		t.Fatalf("phase I: %v", err)
	}

	if err := api.Shutdown(api.ShutdownNormal); err != api.DB_SUCCESS {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := api.Init(); err != api.DB_SUCCESS {
		t.Fatalf("Init after restart: %v", err)
	}
	if err := api.Startup("barracuda"); err != api.DB_SUCCESS {
		t.Fatalf("Startup after restart: %v", err)
	}

	if err := recoverPhase(); err != api.DB_DUPLICATE_KEY {
		t.Fatalf("phase II: expected duplicate key, got %v", err)
	}

	if err := api.TableDrop(nil, recoverTableName()); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(recoverDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
	if err := api.Shutdown(api.ShutdownNormal); err != api.DB_SUCCESS {
		t.Fatalf("Shutdown: %v", err)
	}
}

func recoverPhase() api.ErrCode {
	if err := api.DatabaseCreate(recoverDB); err != api.DB_SUCCESS {
		return err
	}
	if err := createRecoverTable(recoverTableName()); err != api.DB_SUCCESS {
		return err
	}

	dups := 0
	for i := 0; i < recoverTrxCount; i++ {
		trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
		if trx == nil {
			return api.DB_ERROR
		}
		var crsr *api.Cursor
		if err := api.CursorOpenTable(recoverTableName(), trx, &crsr); err != api.DB_SUCCESS {
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.CursorLock(crsr, api.LockIX); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		err := insertRecoverRows(crsr, i*recoverRows, recoverRows)
		if err == api.DB_DUPLICATE_KEY {
			dups++
		} else if err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
			return err
		}
	}

	switch dups {
	case 0:
		return api.DB_SUCCESS
	case recoverTrxCount:
		return api.DB_DUPLICATE_KEY
	default:
		return api.DB_ERROR
	}
}

func createRecoverTable(tableName string) api.ErrCode {
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_INT, api.IB_COL_NONE, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c2", api.IB_VARCHAR, api.IB_COL_NONE, 0, recoverC2MaxLen); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c3", api.IB_BLOB, api.IB_COL_NONE, 0, 0); err != api.DB_SUCCESS {
		return err
	}
	var idx *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "c1", &idx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "c1", 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaSetClustered(idx); err != api.DB_SUCCESS {
		return err
	}

	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if trx == nil {
		return api.DB_ERROR
	}
	if err := api.SchemaLockExclusive(trx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	err := api.TableCreate(trx, schema, nil)
	api.TableSchemaDelete(schema)
	if err != api.DB_SUCCESS && err != api.DB_TABLE_IS_BEING_USED {
		_ = api.TrxRollback(trx)
		return err
	}
	return api.TrxCommit(trx)
}

func insertRecoverRows(crsr *api.Cursor, start, count int) api.ErrCode {
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	rng := rand.New(rand.NewSource(1))
	dups := 0
	for i := start; i < start+count; i++ {
		if err := api.TupleWriteI32(tpl, 0, int32(i)); err != api.DB_SUCCESS {
			return err
		}
		c2 := randText(rng, recoverC2MaxLen)
		if err := api.ColSetValue(tpl, 1, c2, len(c2)); err != api.DB_SUCCESS {
			return err
		}
		c3 := randText(rng, recoverC3MaxLen)
		if err := api.ColSetValue(tpl, 2, c3, len(c3)); err != api.DB_SUCCESS {
			return err
		}
		err := api.CursorInsertRow(crsr, tpl)
		if err == api.DB_DUPLICATE_KEY {
			dups++
		} else if err != api.DB_SUCCESS {
			return err
		}
		tpl = api.TupleClear(tpl)
	}
	if dups == count {
		return api.DB_DUPLICATE_KEY
	}
	if dups != 0 {
		return api.DB_ERROR
	}
	return api.DB_SUCCESS
}

func recoverTableName() string {
	return recoverDB + "/" + recoverTable
}
