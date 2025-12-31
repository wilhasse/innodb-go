package tests

import (
	"fmt"
	"sync"
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	deadlockDB      = "test"
	deadlockTable1  = "T1"
	deadlockTable2  = "T2"
	deadlockRows    = 10
	deadlockThreads = 2
)

func TestDeadlockHarness(t *testing.T) {
	resetAPI(t)
	if err := api.Init(); err != api.DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	defer func() {
		_ = api.Shutdown(api.ShutdownNormal)
	}()

	if err := api.CfgSet("open_files", uint64(8192)); err != api.DB_SUCCESS {
		t.Fatalf("CfgSet open_files: %v", err)
	}
	if err := api.CfgSet("lock_wait_timeout", 3); err != api.DB_SUCCESS {
		t.Fatalf("CfgSet lock_wait_timeout: %v", err)
	}
	if err := api.Startup("barracuda"); err != api.DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}
	if err := api.DatabaseCreate(deadlockDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	if err := createDeadlockTable(deadlockTable1); err != api.DB_SUCCESS {
		t.Fatalf("create table %s: %v", deadlockTable1, err)
	}
	if err := createDeadlockTable(deadlockTable2); err != api.DB_SUCCESS {
		t.Fatalf("create table %s: %v", deadlockTable2, err)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, deadlockThreads)
	for i := 0; i < deadlockThreads; i++ {
		threadID := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := runDeadlockWorker(threadID); err != nil {
				errs <- fmt.Errorf("thread %d: %w", threadID, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := api.TableDrop(nil, fmt.Sprintf("%s/%s", deadlockDB, deadlockTable1)); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop %s: %v", deadlockTable1, err)
	}
	if err := api.TableDrop(nil, fmt.Sprintf("%s/%s", deadlockDB, deadlockTable2)); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop %s: %v", deadlockTable2, err)
	}
	if err := api.DatabaseDrop(deadlockDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createDeadlockTable(name string) api.ErrCode {
	fullName := fmt.Sprintf("%s/%s", deadlockDB, name)
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(fullName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c2", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	var idx *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "PRIMARY", &idx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "c1", 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaSetClustered(idx); err != api.DB_SUCCESS {
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

func runDeadlockWorker(threadID int) error {
	table1 := fmt.Sprintf("%s/%s", deadlockDB, deadlockTable1)
	table2 := fmt.Sprintf("%s/%s", deadlockDB, deadlockTable2)
	var crsr1 *api.Cursor
	var crsr2 *api.Cursor
	if err := api.CursorOpenTable(table1, nil, &crsr1); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CursorOpenTable(table2, nil, &crsr2); err != api.DB_SUCCESS {
		_ = api.CursorClose(crsr1)
		return err
	}

	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	_ = api.CursorAttachTrx(crsr1, trx)
	_ = api.CursorAttachTrx(crsr2, trx)

	start := uint32(threadID * deadlockRows)
	var err api.ErrCode
	if threadID%2 == 0 {
		if err = api.CursorLock(crsr1, api.LockIX); err == api.DB_SUCCESS {
			err = api.CursorLock(crsr2, api.LockIX)
		}
		if err == api.DB_SUCCESS {
			err = insertDeadlockRows(crsr1, start, deadlockRows, uint32(threadID))
		}
		if err == api.DB_SUCCESS {
			err = insertDeadlockRows(crsr2, start, deadlockRows, uint32(threadID))
		}
	} else {
		if err = api.CursorLock(crsr2, api.LockIX); err == api.DB_SUCCESS {
			err = api.CursorLock(crsr1, api.LockIX)
		}
		if err == api.DB_SUCCESS {
			err = insertDeadlockRows(crsr2, start, deadlockRows, uint32(threadID))
		}
		if err == api.DB_SUCCESS {
			err = insertDeadlockRows(crsr1, start, deadlockRows, uint32(threadID))
		}
	}

	_ = api.CursorReset(crsr1)
	_ = api.CursorReset(crsr2)

	if err == api.DB_SUCCESS && api.TrxStateGet(trx) == api.IB_TRX_ACTIVE {
		if err = api.TrxCommit(trx); err != api.DB_SUCCESS {
			_ = api.TrxRelease(trx)
		} else {
			_ = api.TrxRelease(trx)
		}
	} else {
		_ = api.TrxRelease(trx)
	}

	_ = api.CursorClose(crsr1)
	_ = api.CursorClose(crsr2)

	if err != api.DB_SUCCESS {
		return err
	}
	return nil
}

func insertDeadlockRows(crsr *api.Cursor, start uint32, nValues int, threadID uint32) api.ErrCode {
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return api.DB_ERROR
	}
	defer api.TupleDelete(tpl)
	for i := start; i < start+uint32(nValues); i++ {
		if err := api.TupleWriteU32(tpl, 0, i); err != api.DB_SUCCESS {
			return err
		}
		if err := api.TupleWriteU32(tpl, 1, threadID); err != api.DB_SUCCESS {
			return err
		}
		if err := api.CursorInsertRow(crsr, tpl); err != api.DB_SUCCESS {
			return err
		}
	}
	return api.DB_SUCCESS
}
