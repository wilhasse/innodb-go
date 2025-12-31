package tests

import (
	"sort"
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	updateDB    = "update_db"
	updateTable = "t"
)

func TestUpdateHarness(t *testing.T) {
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
	if err := api.DatabaseCreate(updateDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	if err := createUpdateTable(updateTableName()); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}

	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if trx == nil {
		t.Fatalf("TrxBegin returned nil")
	}
	var crsr *api.Cursor
	if err := api.CursorOpenTable(updateTableName(), trx, &crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorOpenTable: %v", err)
	}
	if err := api.CursorLock(crsr, api.LockIX); err != api.DB_SUCCESS {
		t.Fatalf("CursorLock: %v", err)
	}

	if err := insertUpdateRows(crsr); err != api.DB_SUCCESS {
		t.Fatalf("insert rows: %v", err)
	}
	values, err := scanUpdateRows(crsr)
	if err != api.DB_SUCCESS {
		t.Fatalf("scan rows: %v", err)
	}
	if !equalInts(values, []int{0, 2, 4, 6, 8}) {
		t.Fatalf("before update values=%v", values)
	}

	if err := updateAllRows(crsr); err != api.DB_SUCCESS {
		t.Fatalf("update rows: %v", err)
	}
	values, err = scanUpdateRows(crsr)
	if err != api.DB_SUCCESS {
		t.Fatalf("scan rows after update: %v", err)
	}
	if !equalInts(values, []int{0, 1, 2, 3, 4}) {
		t.Fatalf("after update values=%v", values)
	}

	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorClose: %v", err)
	}
	if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
		t.Fatalf("TrxCommit: %v", err)
	}
	if err := api.TableDrop(nil, updateTableName()); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(updateDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createUpdateTable(tableName string) api.ErrCode {
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_INT, api.IB_COL_NONE, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c2", api.IB_VARCHAR, api.IB_COL_NONE, 0, 10); err != api.DB_SUCCESS {
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
	err := api.TableCreate(nil, schema, nil)
	api.TableSchemaDelete(schema)
	return err
}

func insertUpdateRows(crsr *api.Cursor) api.ErrCode {
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	ch := byte('a')
	for i := 0; i < 10; i += 2 {
		if err := api.TupleWriteI32(tpl, 0, int32(i)); err != api.DB_SUCCESS {
			return err
		}
		if err := api.ColSetValue(tpl, 1, []byte{ch}, 1); err != api.DB_SUCCESS {
			return err
		}
		if err := api.CursorInsertRow(crsr, tpl); err != api.DB_SUCCESS {
			return err
		}
		ch++
		tpl = api.TupleClear(tpl)
	}
	return api.DB_SUCCESS
}

func updateAllRows(crsr *api.Cursor) api.ErrCode {
	if err := api.CursorSetLockMode(crsr, api.LockIX); err != api.DB_SUCCESS {
		return err
	}
	if err := api.CursorFirst(crsr); err != api.DB_SUCCESS {
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
			break
		}
		if err != api.DB_SUCCESS {
			return err
		}
		if err := api.TupleCopy(newTpl, oldTpl); err != api.DB_SUCCESS {
			return err
		}
		var c1 int32
		if err := api.TupleReadI32(newTpl, 0, &c1); err != api.DB_SUCCESS {
			return err
		}
		c1 /= 2
		if err := api.TupleWriteI32(newTpl, 0, c1); err != api.DB_SUCCESS {
			return err
		}
		if err := api.CursorUpdateRow(crsr, oldTpl, newTpl); err != api.DB_SUCCESS {
			return err
		}
		if err := api.CursorNext(crsr); err != api.DB_SUCCESS {
			if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
				break
			}
			return err
		}
		oldTpl = api.TupleClear(oldTpl)
		newTpl = api.TupleClear(newTpl)
	}
	return api.DB_SUCCESS
}

func scanUpdateRows(crsr *api.Cursor) ([]int, api.ErrCode) {
	if err := api.CursorFirst(crsr); err != api.DB_SUCCESS {
		if err == api.DB_RECORD_NOT_FOUND {
			return nil, api.DB_SUCCESS
		}
		return nil, err
	}
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return nil, api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	var out []int
	for {
		err := api.CursorReadRow(crsr, tpl)
		if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
			break
		}
		if err != api.DB_SUCCESS {
			return nil, err
		}
		var c1 int32
		if err := api.TupleReadI32(tpl, 0, &c1); err != api.DB_SUCCESS {
			return nil, err
		}
		out = append(out, int(c1))
		if err := api.CursorNext(crsr); err != api.DB_SUCCESS {
			if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
				break
			}
			return nil, err
		}
		tpl = api.TupleClear(tpl)
	}
	sort.Ints(out)
	return out, api.DB_SUCCESS
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func updateTableName() string {
	return updateDB + "/" + updateTable
}
