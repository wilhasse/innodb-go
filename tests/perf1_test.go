package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/wilhasse/innodb-go/api"
)

const (
	perfDB     = "test"
	perfTable1 = "perf_t1"
	perfTable2 = "perf_t2"
	perfRows   = 100
)

func TestPerf1Harness(t *testing.T) {
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
	api.StatsEnable(true)
	api.StatsReset()
	if err := api.DatabaseCreate(perfDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}

	t1 := fmt.Sprintf("%s/%s", perfDB, perfTable1)
	t2 := fmt.Sprintf("%s/%s", perfDB, perfTable2)
	if err := createPerfTable(t1); err != api.DB_SUCCESS {
		t.Fatalf("create t1: %v", err)
	}
	if err := createPerfTable(t2); err != api.DB_SUCCESS {
		t.Fatalf("create t2: %v", err)
	}
	start := time.Now()
	if err := insertPerfRows(t1, perfRows); err != api.DB_SUCCESS {
		t.Fatalf("insert t1: %v", err)
	}
	api.StatsCollect(api.OpInsert, time.Since(start))
	start = time.Now()
	if err := copyPerfRows(t1, t2); err != api.DB_SUCCESS {
		t.Fatalf("copy rows: %v", err)
	}
	api.StatsCollect(api.OpCopy, time.Since(start))
	start = time.Now()
	count, err := joinPerfCount(t1, t2)
	if err != api.DB_SUCCESS {
		t.Fatalf("join count: %v", err)
	}
	api.StatsCollect(api.OpJoin, time.Since(start))
	if count != perfRows {
		t.Fatalf("join count=%d want=%d", count, perfRows)
	}

	if err := api.TableDrop(nil, t1); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop t1: %v", err)
	}
	if err := api.TableDrop(nil, t2); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop t2: %v", err)
	}
	if err := api.DatabaseDrop(perfDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}

	stats := api.StatsSnapshot()
	if stats.Insert.Count != 1 || stats.Copy.Count != 1 || stats.Join.Count != 1 {
		t.Fatalf("stats=%+v", stats)
	}
}

func createPerfTable(name string) api.ErrCode {
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(name, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
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

func insertPerfRows(tableName string, n int) api.ErrCode {
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, trx, &crsr); err != api.DB_SUCCESS {
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
	for i := 1; i <= n; i++ {
		if err := api.TupleWriteU32(tpl, 0, uint32(i)); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.TupleWriteU32(tpl, 1, uint32(i*10)); err != api.DB_SUCCESS {
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.CursorInsertRow(crsr, tpl); err != api.DB_SUCCESS {
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

func copyPerfRows(src, dst string) api.ErrCode {
	var srcCrsr *api.Cursor
	if err := api.CursorOpenTable(src, nil, &srcCrsr); err != api.DB_SUCCESS {
		return err
	}
	defer api.CursorClose(srcCrsr)
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	var dstCrsr *api.Cursor
	if err := api.CursorOpenTable(dst, trx, &dstCrsr); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	defer api.CursorClose(dstCrsr)

	readTpl := api.ClustReadTupleCreate(srcCrsr)
	writeTpl := api.ClustReadTupleCreate(dstCrsr)
	if readTpl == nil || writeTpl == nil {
		_ = api.TrxRollback(trx)
		return api.DB_ERROR
	}
	defer api.TupleDelete(readTpl)
	defer api.TupleDelete(writeTpl)

	err := api.CursorFirst(srcCrsr)
	if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
		_ = api.TrxCommit(trx)
		return api.DB_SUCCESS
	}
	if err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	for {
		err = api.CursorReadRow(srcCrsr, readTpl)
		if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
			break
		}
		if err != api.DB_SUCCESS {
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.TupleCopy(writeTpl, readTpl); err != api.DB_SUCCESS {
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.CursorInsertRow(dstCrsr, writeTpl); err != api.DB_SUCCESS {
			_ = api.TrxRollback(trx)
			return err
		}
		err = api.CursorNext(srcCrsr)
		if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
			break
		}
		if err != api.DB_SUCCESS {
			_ = api.TrxRollback(trx)
			return err
		}
		readTpl = api.TupleClear(readTpl)
		writeTpl = api.TupleClear(writeTpl)
	}
	return api.TrxCommit(trx)
}

func joinPerfCount(t1, t2 string) (int, api.ErrCode) {
	var crsr1 *api.Cursor
	if err := api.CursorOpenTable(t1, nil, &crsr1); err != api.DB_SUCCESS {
		return 0, err
	}
	defer api.CursorClose(crsr1)
	var crsr2 *api.Cursor
	if err := api.CursorOpenTable(t2, nil, &crsr2); err != api.DB_SUCCESS {
		return 0, err
	}
	defer api.CursorClose(crsr2)

	keys := make(map[uint32]struct{})
	tpl := api.ClustReadTupleCreate(crsr1)
	if tpl == nil {
		return 0, api.DB_ERROR
	}
	defer api.TupleDelete(tpl)
	err := api.CursorFirst(crsr1)
	for err == api.DB_SUCCESS {
		if err = api.CursorReadRow(crsr1, tpl); err != api.DB_SUCCESS {
			break
		}
		var key uint32
		if err := api.TupleReadU32(tpl, 0, &key); err != api.DB_SUCCESS {
			return 0, err
		}
		keys[key] = struct{}{}
		err = api.CursorNext(crsr1)
		tpl = api.TupleClear(tpl)
	}
	if err != api.DB_END_OF_INDEX && err != api.DB_RECORD_NOT_FOUND && err != api.DB_SUCCESS {
		return 0, err
	}

	count := 0
	tpl2 := api.ClustReadTupleCreate(crsr2)
	if tpl2 == nil {
		return 0, api.DB_ERROR
	}
	defer api.TupleDelete(tpl2)
	err = api.CursorFirst(crsr2)
	for err == api.DB_SUCCESS {
		if err = api.CursorReadRow(crsr2, tpl2); err != api.DB_SUCCESS {
			break
		}
		var key uint32
		if err := api.TupleReadU32(tpl2, 0, &key); err != api.DB_SUCCESS {
			return count, err
		}
		if _, ok := keys[key]; ok {
			count++
		}
		err = api.CursorNext(crsr2)
		tpl2 = api.TupleClear(tpl2)
	}
	if err != api.DB_END_OF_INDEX && err != api.DB_RECORD_NOT_FOUND && err != api.DB_SUCCESS {
		return count, err
	}
	return count, api.DB_SUCCESS
}
