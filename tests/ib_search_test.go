package tests

import (
	"testing"

	"github.com/wilhasse/innodb-go/api"
	"github.com/wilhasse/innodb-go/data"
)

const (
	searchDB    = "search_test"
	searchTable = "t"
)

type searchRow struct {
	c1 string
	c2 string
	c3 uint32
}

var searchRows = []searchRow{
	{c1: "abc", c2: "def", c3: 1},
	{c1: "abc", c2: "zzz", c3: 1},
	{c1: "ghi", c2: "jkl", c3: 2},
	{c1: "mno", c2: "pqr", c3: 3},
	{c1: "mno", c2: "xxx", c3: 3},
	{c1: "stu", c2: "vwx", c3: 4},
}

func TestSearchHarness(t *testing.T) {
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
	if err := api.DatabaseCreate(searchDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	if err := createSearchTable(searchTableName()); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}

	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if trx == nil {
		t.Fatalf("TrxBegin returned nil")
	}
	var crsr *api.Cursor
	if err := api.CursorOpenTable(searchTableName(), trx, &crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorOpenTable: %v", err)
	}
	if err := api.CursorLock(crsr, api.LockIX); err != api.DB_SUCCESS {
		t.Fatalf("CursorLock: %v", err)
	}
	if err := insertSearchRows(crsr); err != api.DB_SUCCESS {
		t.Fatalf("insert rows: %v", err)
	}

	ret, err := moveToSearch(crsr, "abc", "def", api.IB_EXACT_MATCH)
	if err != api.DB_SUCCESS {
		t.Fatalf("moveto1: %v", err)
	}
	if ret != 0 {
		t.Fatalf("moveto1 ret=%d, want 0", ret)
	}
	count, err := scanSearchRows(crsr, func(tpl *data.Tuple) (bool, bool, api.ErrCode) {
		c1, err := tupleString(tpl, 0)
		if err != api.DB_SUCCESS {
			return false, false, err
		}
		c2, err := tupleString(tpl, 1)
		if err != api.DB_SUCCESS {
			return false, false, err
		}
		if c1 != "abc" || c2 != "def" {
			return false, false, api.DB_ERROR
		}
		return true, false, api.DB_SUCCESS
	})
	if err != api.DB_SUCCESS {
		t.Fatalf("query1: %v", err)
	}
	if count != 1 {
		t.Fatalf("query1 count=%d, want 1", count)
	}

	ret, err = moveToSearch(crsr, "abc", "", api.IB_CLOSEST_MATCH)
	if err != api.DB_SUCCESS {
		t.Fatalf("moveto2: %v", err)
	}
	if ret != -1 {
		t.Fatalf("moveto2 ret=%d, want -1", ret)
	}
	count, err = scanSearchRows(crsr, func(tpl *data.Tuple) (bool, bool, api.ErrCode) {
		c1, err := tupleString(tpl, 0)
		if err != api.DB_SUCCESS {
			return false, false, err
		}
		if c1 != "abc" {
			return false, false, api.DB_SUCCESS
		}
		return true, true, api.DB_SUCCESS
	})
	if err != api.DB_SUCCESS {
		t.Fatalf("query2: %v", err)
	}
	if count != 2 {
		t.Fatalf("query2 count=%d, want 2", count)
	}

	ret, err = moveToSearch(crsr, "g", "", api.IB_CLOSEST_MATCH)
	if err != api.DB_SUCCESS {
		t.Fatalf("moveto3: %v", err)
	}
	if ret != -1 {
		t.Fatalf("moveto3 ret=%d, want -1", ret)
	}
	count, err = scanSearchRows(crsr, func(tpl *data.Tuple) (bool, bool, api.ErrCode) {
		c1, err := tupleString(tpl, 0)
		if err != api.DB_SUCCESS {
			return false, false, err
		}
		if c1 < "g" {
			return false, false, api.DB_ERROR
		}
		return true, true, api.DB_SUCCESS
	})
	if err != api.DB_SUCCESS {
		t.Fatalf("query3: %v", err)
	}
	if count != 4 {
		t.Fatalf("query3 count=%d, want 4", count)
	}

	ret, err = moveToSearch(crsr, "mno", "x", api.IB_EXACT_PREFIX)
	if err != api.DB_SUCCESS {
		t.Fatalf("moveto4: %v", err)
	}
	if ret != -1 {
		t.Fatalf("moveto4 ret=%d, want -1", ret)
	}
	count, err = scanSearchRows(crsr, func(tpl *data.Tuple) (bool, bool, api.ErrCode) {
		c1, err := tupleString(tpl, 0)
		if err != api.DB_SUCCESS {
			return false, false, err
		}
		c2, err := tupleString(tpl, 1)
		if err != api.DB_SUCCESS {
			return false, false, err
		}
		if c1 != "mno" {
			return false, false, api.DB_SUCCESS
		}
		if c2 < "x" {
			return false, false, api.DB_ERROR
		}
		return true, true, api.DB_SUCCESS
	})
	if err != api.DB_SUCCESS {
		t.Fatalf("query4: %v", err)
	}
	if count != 1 {
		t.Fatalf("query4 count=%d, want 1", count)
	}

	_, err = moveToSearch(crsr, "mno", "z", api.IB_EXACT_PREFIX)
	if err != api.DB_RECORD_NOT_FOUND {
		t.Fatalf("moveto5 err=%v, want DB_RECORD_NOT_FOUND", err)
	}

	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorClose: %v", err)
	}
	if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
		t.Fatalf("TrxCommit: %v", err)
	}
	if err := api.TableDrop(nil, searchTableName()); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(searchDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createSearchTable(tableName string) api.ErrCode {
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_VARCHAR, api.IB_COL_NONE, 0, 31); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c2", api.IB_VARCHAR, api.IB_COL_NONE, 0, 31); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c3", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	var idx *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "PRIMARY_KEY", &idx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "c1", 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "c2", 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaSetClustered(idx); err != api.DB_SUCCESS {
		return err
	}
	err := api.TableCreate(nil, schema, nil)
	api.TableSchemaDelete(schema)
	return err
}

func insertSearchRows(crsr *api.Cursor) api.ErrCode {
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	for _, row := range searchRows {
		if err := api.ColSetValue(tpl, 0, []byte(row.c1), len(row.c1)); err != api.DB_SUCCESS {
			return err
		}
		if err := api.ColSetValue(tpl, 1, []byte(row.c2), len(row.c2)); err != api.DB_SUCCESS {
			return err
		}
		if err := api.TupleWriteU32(tpl, 2, row.c3); err != api.DB_SUCCESS {
			return err
		}
		if err := api.CursorInsertRow(crsr, tpl); err != api.DB_SUCCESS {
			return err
		}
		tpl = api.TupleClear(tpl)
	}
	return api.DB_SUCCESS
}

func moveToSearch(crsr *api.Cursor, c1, c2 string, mode api.MatchMode) (int, api.ErrCode) {
	if err := api.CursorSetMatchMode(crsr, mode); err != api.DB_SUCCESS {
		return 0, err
	}
	key := api.SecSearchTupleCreate(crsr)
	if key == nil {
		return 0, api.DB_ERROR
	}
	defer api.TupleDelete(key)

	if err := api.ColSetValue(key, 0, []byte(c1), len(c1)); err != api.DB_SUCCESS {
		return 0, err
	}
	if c2 != "" {
		if err := api.ColSetValue(key, 1, []byte(c2), len(c2)); err != api.DB_SUCCESS {
			return 0, err
		}
	}
	var ret int
	err := api.CursorMoveTo(crsr, key, api.CursorGE, &ret)
	return ret, err
}

func scanSearchRows(crsr *api.Cursor, fn func(*data.Tuple) (bool, bool, api.ErrCode)) (int, api.ErrCode) {
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return 0, api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	count := 0
	for {
		err := api.CursorReadRow(crsr, tpl)
		if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
			return count, api.DB_SUCCESS
		}
		if err != api.DB_SUCCESS {
			return count, err
		}
		add, cont, err := fn(tpl)
		if err != api.DB_SUCCESS {
			return count, err
		}
		if add {
			count++
		}
		if !cont {
			return count, api.DB_SUCCESS
		}
		if err := api.CursorNext(crsr); err != api.DB_SUCCESS {
			if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
				return count, api.DB_SUCCESS
			}
			return count, err
		}
		tpl = api.TupleClear(tpl)
	}
}

func tupleString(tpl *data.Tuple, col int) (string, api.ErrCode) {
	val := api.ColGetValue(tpl, col)
	if val == nil {
		return "", api.DB_ERROR
	}
	return string(val), api.DB_SUCCESS
}

func searchTableName() string {
	return searchDB + "/" + searchTable
}
