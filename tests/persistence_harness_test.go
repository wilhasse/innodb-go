package tests

import (
	"sort"
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

// TestPersistenceHarness is a comprehensive test that exercises the full
// persistence stack: buffer pool I/O, page allocation, dict persistence,
// and restart recovery. It verifies that data survives multiple restart cycles.
func TestPersistenceHarness(t *testing.T) {
	resetAPI(t)
	dir := t.TempDir() + "/"

	const (
		dbName    = "persist_harness"
		tableName = dbName + "/data"
	)

	// Phase 1: Initialize, create schema and insert data
	t.Run("Phase1_CreateAndPopulate", func(t *testing.T) {
		if err := api.Init(); err != api.DB_SUCCESS {
			t.Fatalf("Init: %v", err)
		}
		if err := api.CfgSet("data_home_dir", dir); err != api.DB_SUCCESS {
			t.Fatalf("CfgSet data_home_dir: %v", err)
		}
		if err := api.Startup("barracuda"); err != api.DB_SUCCESS {
			t.Fatalf("Startup: %v", err)
		}
		if err := api.DatabaseCreate(dbName); err != api.DB_SUCCESS {
			t.Fatalf("DatabaseCreate: %v", err)
		}

		if err := createPersistHarnessTable(tableName); err != api.DB_SUCCESS {
			t.Fatalf("create table: %v", err)
		}
		if err := insertPersistHarnessRows(tableName, []uint32{10, 20, 30, 40, 50}); err != api.DB_SUCCESS {
			t.Fatalf("insert rows: %v", err)
		}

		if err := api.Shutdown(api.ShutdownNormal); err != api.DB_SUCCESS {
			t.Fatalf("Shutdown: %v", err)
		}
	})

	// Phase 2: Restart and verify data persisted
	t.Run("Phase2_RestartAndVerify", func(t *testing.T) {
		if err := api.Init(); err != api.DB_SUCCESS {
			t.Fatalf("Init after restart: %v", err)
		}
		if err := api.CfgSet("data_home_dir", dir); err != api.DB_SUCCESS {
			t.Fatalf("CfgSet data_home_dir: %v", err)
		}
		if err := api.Startup("barracuda"); err != api.DB_SUCCESS {
			t.Fatalf("Startup after restart: %v", err)
		}
		if err := api.DatabaseCreate(dbName); err != api.DB_SUCCESS {
			t.Fatalf("DatabaseCreate after restart: %v", err)
		}

		// Verify schema persisted
		if err := verifyPersistHarnessSchema(tableName); err != api.DB_SUCCESS {
			t.Fatalf("verify schema: %v", err)
		}

		// Verify data persisted
		if err := verifyPersistHarnessRows(tableName, []uint32{10, 20, 30, 40, 50}); err != api.DB_SUCCESS {
			t.Fatalf("verify rows: %v", err)
		}

		// Insert more data
		if err := insertPersistHarnessRows(tableName, []uint32{60, 70}); err != api.DB_SUCCESS {
			t.Fatalf("insert additional rows: %v", err)
		}

		if err := api.Shutdown(api.ShutdownNormal); err != api.DB_SUCCESS {
			t.Fatalf("Shutdown: %v", err)
		}
	})

	// Phase 3: Second restart to verify incremental changes
	t.Run("Phase3_SecondRestartVerify", func(t *testing.T) {
		if err := api.Init(); err != api.DB_SUCCESS {
			t.Fatalf("Init after second restart: %v", err)
		}
		if err := api.CfgSet("data_home_dir", dir); err != api.DB_SUCCESS {
			t.Fatalf("CfgSet data_home_dir: %v", err)
		}
		if err := api.Startup("barracuda"); err != api.DB_SUCCESS {
			t.Fatalf("Startup after second restart: %v", err)
		}
		if err := api.DatabaseCreate(dbName); err != api.DB_SUCCESS {
			t.Fatalf("DatabaseCreate after second restart: %v", err)
		}

		// Verify all data including incremental inserts
		if err := verifyPersistHarnessRows(tableName, []uint32{10, 20, 30, 40, 50, 60, 70}); err != api.DB_SUCCESS {
			t.Fatalf("verify all rows: %v", err)
		}

		// Cleanup
		if err := api.TableDrop(nil, tableName); err != api.DB_SUCCESS {
			t.Fatalf("TableDrop: %v", err)
		}
		if err := api.DatabaseDrop(dbName); err != api.DB_SUCCESS {
			t.Fatalf("DatabaseDrop: %v", err)
		}
		if err := api.Shutdown(api.ShutdownNormal); err != api.DB_SUCCESS {
			t.Fatalf("Shutdown final: %v", err)
		}
	})
}

func createPersistHarnessTable(tableName string) api.ErrCode {
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	// Primary key column
	if err := api.TableSchemaAddCol(schema, "id", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	// Data column
	if err := api.TableSchemaAddCol(schema, "value", api.IB_VARCHAR, api.IB_COL_NONE, 0, 64); err != api.DB_SUCCESS {
		return err
	}

	// Primary index
	var primary *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "PRIMARY", &primary); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(primary, "id", 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaSetClustered(primary); err != api.DB_SUCCESS {
		return err
	}

	// Secondary index on value
	var secondary *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "idx_value", &secondary); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(secondary, "value", 0); err != api.DB_SUCCESS {
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
	if err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	return api.TrxCommit(trx)
}

func insertPersistHarnessRows(tableName string, ids []uint32) api.ErrCode {
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if trx == nil {
		return api.DB_ERROR
	}
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
	for _, id := range ids {
		if err := api.TupleWriteU32(tpl, 0, id); err != api.DB_SUCCESS {
			api.TupleDelete(tpl)
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		val := []byte("data_" + string(rune('0'+id/10)))
		if err := api.ColSetValue(tpl, 1, val, len(val)); err != api.DB_SUCCESS {
			api.TupleDelete(tpl)
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		if err := api.CursorInsertRow(crsr, tpl); err != api.DB_SUCCESS {
			api.TupleDelete(tpl)
			_ = api.CursorClose(crsr)
			_ = api.TrxRollback(trx)
			return err
		}
		tpl = api.TupleClear(tpl)
	}
	api.TupleDelete(tpl)
	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	return api.TrxCommit(trx)
}

func verifyPersistHarnessRows(tableName string, expected []uint32) api.ErrCode {
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, nil, &crsr); err != api.DB_SUCCESS {
		return err
	}
	defer func() {
		_ = api.CursorClose(crsr)
	}()
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	if err := api.CursorFirst(crsr); err == api.DB_END_OF_INDEX {
		if len(expected) == 0 {
			return api.DB_SUCCESS
		}
		return api.DB_ERROR
	} else if err != api.DB_SUCCESS {
		return err
	}

	got := make([]uint32, 0)
	for {
		if err := api.CursorReadRow(crsr, tpl); err != api.DB_SUCCESS {
			return err
		}
		var val uint32
		if err := api.TupleReadU32(tpl, 0, &val); err != api.DB_SUCCESS {
			return err
		}
		got = append(got, val)
		if err := api.CursorNext(crsr); err == api.DB_END_OF_INDEX {
			break
		} else if err != api.DB_SUCCESS {
			return err
		}
	}

	if len(got) != len(expected) {
		return api.DB_ERROR
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	sort.Slice(expected, func(i, j int) bool { return expected[i] < expected[j] })
	for i, val := range expected {
		if got[i] != val {
			return api.DB_ERROR
		}
	}
	return api.DB_SUCCESS
}

func verifyPersistHarnessSchema(tableName string) api.ErrCode {
	trx := api.TrxBegin(api.IB_TRX_SERIALIZABLE)
	if trx == nil {
		return api.DB_ERROR
	}
	if err := api.SchemaLockExclusive(trx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}

	var cols []string
	var idxNames []string
	visitor := api.SchemaVisitor{
		VisitTable: func(_ any, _ string, _ api.TableFormat, _ api.Ulint, nCols int, nIndexes int) int {
			if nCols != 2 || nIndexes != 2 {
				return 1
			}
			return 0
		},
		VisitColumn: func(_ any, name string, _ api.ColType, _ api.Ulint, _ api.ColAttr) int {
			cols = append(cols, name)
			return 0
		},
		VisitIndex: func(_ any, name string, _ api.Bool, _ api.Bool, _ int) int {
			idxNames = append(idxNames, name)
			return 0
		},
	}
	if err := api.TableSchemaVisit(trx, tableName, &visitor, nil); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}

	sort.Strings(cols)
	sort.Strings(idxNames)

	if len(cols) != 2 || cols[0] != "id" || cols[1] != "value" {
		_ = api.TrxRollback(trx)
		return api.DB_ERROR
	}
	if len(idxNames) != 2 || idxNames[0] != "PRIMARY" || idxNames[1] != "idx_value" {
		_ = api.TrxRollback(trx)
		return api.DB_ERROR
	}

	return api.TrxCommit(trx)
}
