package tests

import (
	"fmt"
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	cursorDB    = "test"
	cursorTable = "t"
)

func TestCursorHarness(t *testing.T) {
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
	if err := api.DatabaseCreate(cursorDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	if err := createCursorTable(cursorDB, cursorTable); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}

	tableName := fmt.Sprintf("%s/%s", cursorDB, cursorTable)
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, nil, &crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorOpenTable: %v", err)
	}
	if err := api.CursorLock(crsr, api.LockIX); err != api.DB_SUCCESS {
		t.Fatalf("CursorLock: %v", err)
	}
	if err := insertCursorRows(crsr, 10); err != api.DB_SUCCESS {
		t.Fatalf("insert rows: %v", err)
	}

	if err := api.CursorFirst(crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorFirst: %v", err)
	}
	got, err := scanCursor(crsr, nil)
	if err != api.DB_SUCCESS {
		t.Fatalf("scan all: %v", err)
	}
	assertValues(t, got, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

	search := api.ClustSearchTupleCreate(crsr)
	if search == nil {
		t.Fatalf("ClustSearchTupleCreate returned nil")
	}
	if err := api.TupleWriteI32(search, 0, 5); err != api.DB_SUCCESS {
		t.Fatalf("TupleWriteI32: %v", err)
	}
	var ret int
	if err := api.CursorMoveTo(crsr, search, api.CursorGE, &ret); err != api.DB_SUCCESS {
		t.Fatalf("CursorMoveTo GE: %v", err)
	}
	if ret != 0 {
		t.Fatalf("CursorMoveTo GE ret=%d, want 0", ret)
	}
	got, err = scanCursor(crsr, func(val int32) (bool, bool) {
		if val == 5 {
			return true, false
		}
		return false, true
	})
	if err != api.DB_SUCCESS {
		t.Fatalf("scan eq 5: %v", err)
	}
	assertValues(t, got, []int32{5})

	if err := api.CursorMoveTo(crsr, search, api.CursorG, &ret); err != api.DB_SUCCESS {
		t.Fatalf("CursorMoveTo G: %v", err)
	}
	if ret >= 0 {
		t.Fatalf("CursorMoveTo G ret=%d, want <0", ret)
	}
	got, err = scanCursor(crsr, nil)
	if err != api.DB_SUCCESS {
		t.Fatalf("scan > 5: %v", err)
	}
	assertValues(t, got, []int32{6, 7, 8, 9})

	if err := api.CursorFirst(crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorFirst: %v", err)
	}
	got, err = scanCursor(crsr, func(val int32) (bool, bool) {
		if val < 5 {
			return true, false
		}
		return false, true
	})
	if err != api.DB_SUCCESS {
		t.Fatalf("scan < 5: %v", err)
	}
	assertValues(t, got, []int32{0, 1, 2, 3, 4})

	search = api.ClustSearchTupleCreate(crsr)
	if search == nil {
		t.Fatalf("ClustSearchTupleCreate returned nil")
	}
	if err := api.TupleWriteI32(search, 0, 1); err != api.DB_SUCCESS {
		t.Fatalf("TupleWriteI32: %v", err)
	}
	if err := api.CursorMoveTo(crsr, search, api.CursorGE, &ret); err != api.DB_SUCCESS {
		t.Fatalf("CursorMoveTo GE: %v", err)
	}
	if ret != 0 {
		t.Fatalf("CursorMoveTo GE ret=%d, want 0", ret)
	}
	got, err = scanCursor(crsr, func(val int32) (bool, bool) {
		if val < 5 {
			return true, false
		}
		return false, true
	})
	if err != api.DB_SUCCESS {
		t.Fatalf("scan >=1 and <5: %v", err)
	}
	assertValues(t, got, []int32{1, 2, 3, 4})

	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorClose: %v", err)
	}
	if err := api.TableDrop(nil, tableName); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(cursorDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createCursorTable(dbName, tableName string) api.ErrCode {
	fullName := fmt.Sprintf("%s/%s", dbName, tableName)
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(fullName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_INT, api.IB_COL_NONE, 0, 4); err != api.DB_SUCCESS {
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
	if err := api.TableCreate(nil, schema, nil); err != api.DB_SUCCESS {
		return err
	}
	api.TableSchemaDelete(schema)
	return api.DB_SUCCESS
}

func insertCursorRows(crsr *api.Cursor, count int) api.ErrCode {
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return api.DB_ERROR
	}
	for i := 0; i < count; i++ {
		if err := api.TupleWriteI32(tpl, 0, int32(i)); err != api.DB_SUCCESS {
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

func scanCursor(crsr *api.Cursor, selector func(int32) (bool, bool)) ([]int32, api.ErrCode) {
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return nil, api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	var out []int32
	for {
		err := api.CursorReadRow(crsr, tpl)
		if err == api.DB_RECORD_NOT_FOUND || err == api.DB_END_OF_INDEX {
			return out, api.DB_SUCCESS
		}
		if err != api.DB_SUCCESS {
			return out, err
		}
		var val int32
		if err := api.TupleReadI32(tpl, 0, &val); err != api.DB_SUCCESS {
			return out, err
		}
		accept, stop := true, false
		if selector != nil {
			accept, stop = selector(val)
		}
		if accept {
			out = append(out, val)
		}
		if stop {
			return out, api.DB_SUCCESS
		}
		err = api.CursorNext(crsr)
		if err == api.DB_RECORD_NOT_FOUND || err == api.DB_END_OF_INDEX {
			return out, api.DB_SUCCESS
		}
		if err != api.DB_SUCCESS {
			return out, err
		}
		tpl = api.TupleClear(tpl)
	}
}

func assertValues(t *testing.T, got, want []int32) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("values len=%d want=%d got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("values[%d]=%d want=%d got=%v", i, got[i], want[i], got)
		}
	}
}
