package tests

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	ddlDB        = "test"
	ddlTable     = "ib_ddl"
	ddlRows      = 100
	varcharMax   = 10
	blobMax      = 8192
)

func TestDDLHarness(t *testing.T) {
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
	if err := api.DatabaseCreate(ddlDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}

	tableName := fmt.Sprintf("%s/%s", ddlDB, ddlTable)
	if err := createDDLTable(tableName); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}

	if err := insertDDLRows(tableName); err != api.DB_SUCCESS {
		t.Fatalf("insert rows: %v", err)
	}

	if err := createDDLIndexes(tableName); err != api.DB_SUCCESS {
		t.Fatalf("create indexes: %v", err)
	}
	if err := openDDLIndexes(tableName); err != api.DB_SUCCESS {
		t.Fatalf("open indexes: %v", err)
	}

	if err := api.TableDrop(nil, tableName); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(ddlDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createDDLTable(tableName string) api.ErrCode {
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_INT, api.IB_COL_NONE, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TblSchAddVarcharCol(schema, "c2", varcharMax); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TblSchAddBlobCol(schema, "c3"); err != api.DB_SUCCESS {
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

func insertDDLRows(tableName string) api.ErrCode {
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, trx, &crsr); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.CursorLock(crsr, api.LockIX); err != api.DB_SUCCESS {
		_ = api.CursorClose(crsr)
		_ = api.TrxRollback(trx)
		return err
	}
	if err := insertRandomDDLRows(crsr); err != api.DB_SUCCESS {
		_ = api.CursorClose(crsr)
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	return api.TrxCommit(trx)
}

func insertRandomDDLRows(crsr *api.Cursor) api.ErrCode {
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return api.DB_ERROR
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < ddlRows; i++ {
		if err := api.TupleWriteI32(tpl, 0, int32(i%10)); err != api.DB_SUCCESS {
			return err
		}
		name := randText(rng, varcharMax)
		if err := api.ColSetValue(tpl, 1, name, len(name)); err != api.DB_SUCCESS {
			return err
		}
		blob := randText(rng, blobMax)
		if err := api.ColSetValue(tpl, 2, blob, len(blob)); err != api.DB_SUCCESS {
			return err
		}
		if err := api.CursorInsertRow(crsr, tpl); err != api.DB_SUCCESS {
			return err
		}
		tpl = api.TupleClear(tpl)
	}
	api.TupleDelete(tpl)
	return api.DB_SUCCESS
}

func createDDLIndexes(tableName string) api.ErrCode {
	if err := createSecondaryIndex(tableName, "c1", 0); err != api.DB_SUCCESS {
		return err
	}
	if err := createSecondaryIndex(tableName, "c2", 0); err != api.DB_SUCCESS {
		return err
	}
	return createSecondaryIndex(tableName, "c3", 10)
}

func createSecondaryIndex(tableName, colName string, prefix int) api.ErrCode {
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if err := api.SchemaLockExclusive(trx); err != api.DB_SUCCESS {
		return err
	}
	indexName := fmt.Sprintf("%s_%s", tableName, colName)
	var idx *api.IndexSchema
	if err := api.IndexSchemaCreate(trx, indexName, tableName, &idx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.IndexSchemaAddCol(idx, colName, prefix); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	if err := api.IndexCreate(idx, nil); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	api.IndexSchemaDelete(idx)
	return api.TrxCommit(trx)
}

func openDDLIndexes(tableName string) api.ErrCode {
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, trx, &crsr); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	indexNames := []string{
		fmt.Sprintf("%s_%s", tableName, "c1"),
		fmt.Sprintf("%s_%s", tableName, "c2"),
		fmt.Sprintf("%s_%s", tableName, "c3"),
	}
	for _, name := range indexNames {
		var idxCrsr *api.Cursor
		if err := api.CursorOpenIndexUsingName(crsr, name, &idxCrsr); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.CursorClose(idxCrsr); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
	}
	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	return api.TrxCommit(trx)
}

func randText(rng *rand.Rand, max int) []byte {
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	n := rng.Intn(max)
	if n == 0 {
		n = 1
	}
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = charset[rng.Intn(len(charset))]
	}
	return buf
}
