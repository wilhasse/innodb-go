package tests

import (
	"fmt"
	"sync"
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	mtStressDB    = "test"
	mtStressTable = "mt_stress"
	mtWorkers     = 4
	mtRows        = 25
)

func TestMtStressHarness(t *testing.T) {
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
	if err := api.DatabaseCreate(mtStressDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}

	tableName := fmt.Sprintf("%s/%s", mtStressDB, mtStressTable)
	if err := createMtStressTable(tableName); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, mtWorkers)
	var insertMu sync.Mutex
	for i := 0; i < mtWorkers; i++ {
		workerID := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := insertMtStressRows(tableName, workerID, &insertMu); err != api.DB_SUCCESS {
				errs <- fmt.Errorf("worker %d: %v", workerID, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	count, err := countMtStressRows(tableName)
	if err != api.DB_SUCCESS {
		t.Fatalf("count rows: %v", err)
	}
	if count != mtWorkers*mtRows {
		t.Fatalf("row count=%d want=%d", count, mtWorkers*mtRows)
	}

	if err := api.TableDrop(nil, tableName); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(mtStressDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createMtStressTable(tableName string) api.ErrCode {
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TblSchAddU32Col(schema, "A"); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TblSchAddU32Col(schema, "D"); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TblSchAddBlobCol(schema, "B"); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TblSchAddBlobCol(schema, "C"); err != api.DB_SUCCESS {
		return err
	}

	var idx *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "PRIMARY", &idx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "B", 10); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "A", 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "D", 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaSetClustered(idx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaSetUnique(idx); err != api.DB_SUCCESS {
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

func insertMtStressRows(tableName string, workerID int, mu *sync.Mutex) api.ErrCode {
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
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		_ = api.CursorClose(crsr)
		_ = api.TrxRollback(trx)
		return api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	for i := 0; i < mtRows; i++ {
		id := uint32(workerID*mtRows + i + 1)
		if err := api.TupleWriteU32(tpl, 0, id); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.TupleWriteU32(tpl, 1, 5); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		bVal := []byte(fmt.Sprintf("%02d%08d", workerID, i))
		cVal := []byte(fmt.Sprintf("C%02d_%08d", workerID, i))
		if err := api.ColSetValue(tpl, 2, bVal, len(bVal)); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.ColSetValue(tpl, 3, cVal, len(cVal)); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		mu.Lock()
		err := api.CursorInsertRow(crsr, tpl)
		mu.Unlock()
		if err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		tpl = api.TupleClear(tpl)
	}
	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	return api.TrxCommit(trx)
}

func countMtStressRows(tableName string) (int, api.ErrCode) {
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, nil, &crsr); err != api.DB_SUCCESS {
		return 0, err
	}
	defer api.CursorClose(crsr)
	err := api.CursorFirst(crsr)
	if err == api.DB_RECORD_NOT_FOUND || err == api.DB_END_OF_INDEX {
		return 0, api.DB_SUCCESS
	}
	if err != api.DB_SUCCESS {
		return 0, err
	}
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return 0, api.DB_ERROR
	}
	defer api.TupleDelete(tpl)
	count := 0
	for {
		err = api.CursorReadRow(crsr, tpl)
		if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
			break
		}
		if err != api.DB_SUCCESS {
			return count, err
		}
		count++
		err = api.CursorNext(crsr)
		if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
			break
		}
		if err != api.DB_SUCCESS {
			return count, err
		}
		tpl = api.TupleClear(tpl)
	}
	return count, api.DB_SUCCESS
}
