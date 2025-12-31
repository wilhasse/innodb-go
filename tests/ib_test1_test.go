package tests

import (
	"sort"
	"testing"

	"github.com/wilhasse/innodb-go/api"
	"github.com/wilhasse/innodb-go/data"
)

const (
	test1DB    = "test1_db"
	test1Table = "t"
)

type test1Row struct {
	c1 string
	c2 string
	c3 uint32
}

var test1Rows = []test1Row{
	{c1: "a", c2: "t", c3: 1},
	{c1: "b", c2: "u", c3: 2},
	{c1: "c", c2: "b", c3: 3},
	{c1: "d", c2: "n", c3: 4},
	{c1: "e", c2: "s", c3: 5},
	{c1: "e", c2: "j", c3: 6},
	{c1: "d", c2: "f", c3: 7},
	{c1: "c", c2: "n", c3: 8},
	{c1: "b", c2: "z", c3: 9},
	{c1: "a", c2: "i", c3: 10},
}

func TestTest1Harness(t *testing.T) {
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
	if err := api.DatabaseCreate(test1DB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	if err := createTest1Table(test1TableName()); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}

	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if trx == nil {
		t.Fatalf("TrxBegin returned nil")
	}
	var crsr *api.Cursor
	if err := api.CursorOpenTable(test1TableName(), trx, &crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorOpenTable: %v", err)
	}
	if err := api.CursorLock(crsr, api.LockIX); err != api.DB_SUCCESS {
		t.Fatalf("CursorLock: %v", err)
	}

	if err := insertTest1Rows(crsr); err != api.DB_SUCCESS {
		t.Fatalf("insert rows: %v", err)
	}
	count, err := scanTest1Rows(crsr, nil)
	if err != api.DB_SUCCESS {
		t.Fatalf("query rows: %v", err)
	}
	if count != len(test1Rows) {
		t.Fatalf("rows=%d, want %d", count, len(test1Rows))
	}

	if err := updateRowsA(crsr); err != api.DB_SUCCESS {
		t.Fatalf("update rows: %v", err)
	}
	_, err = scanTest1Rows(crsr, func(tpl *data.Tuple) api.ErrCode {
		c1, err := tupleString(tpl, 0)
		if err != api.DB_SUCCESS {
			return err
		}
		if c1 != "a" {
			return api.DB_SUCCESS
		}
		var c3 uint32
		if err := api.TupleReadU32(tpl, 2, &c3); err != api.DB_SUCCESS {
			return err
		}
		if c3 < 100 {
			return api.DB_ERROR
		}
		return api.DB_SUCCESS
	})
	if err != api.DB_SUCCESS {
		t.Fatalf("query after update: %v", err)
	}

	if err := deleteRowBz(crsr); err != api.DB_SUCCESS {
		t.Fatalf("delete row: %v", err)
	}
	count, err = scanTest1Rows(crsr, func(tpl *data.Tuple) api.ErrCode {
		c1, err := tupleString(tpl, 0)
		if err != api.DB_SUCCESS {
			return err
		}
		c2, err := tupleString(tpl, 1)
		if err != api.DB_SUCCESS {
			return err
		}
		if c1 == "b" && c2 == "z" {
			return api.DB_ERROR
		}
		return api.DB_SUCCESS
	})
	if err != api.DB_SUCCESS {
		t.Fatalf("query after delete: %v", err)
	}
	if count != len(test1Rows)-1 {
		t.Fatalf("rows=%d, want %d", count, len(test1Rows)-1)
	}

	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorClose: %v", err)
	}
	if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
		t.Fatalf("TrxCommit: %v", err)
	}
	if err := api.TableDrop(nil, test1TableName()); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(test1DB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createTest1Table(tableName string) api.ErrCode {
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
	if err := api.TableSchemaAddIndex(schema, "c1_c2", &idx); err != api.DB_SUCCESS {
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

func insertTest1Rows(crsr *api.Cursor) api.ErrCode {
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	rows := append([]test1Row(nil), test1Rows...)
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].c1 != rows[j].c1 {
			return rows[i].c1 < rows[j].c1
		}
		return rows[i].c2 < rows[j].c2
	})

	for _, row := range rows {
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

func updateRowsA(crsr *api.Cursor) api.ErrCode {
	if err := api.CursorSetMatchMode(crsr, api.IB_CLOSEST_MATCH); err != api.DB_SUCCESS {
		return err
	}
	key := api.SecSearchTupleCreate(crsr)
	if key == nil {
		return api.DB_ERROR
	}
	if err := api.ColSetValue(key, 0, []byte("a"), 1); err != api.DB_SUCCESS {
		api.TupleDelete(key)
		return err
	}
	var res int
	err := api.CursorMoveTo(crsr, key, api.CursorGE, &res)
	api.TupleDelete(key)
	if err != api.DB_SUCCESS {
		return err
	}
	if res != -1 {
		return api.DB_ERROR
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
		if err := api.CursorReadRow(crsr, oldTpl); err != api.DB_SUCCESS {
			return err
		}
		c1, err := tupleString(oldTpl, 0)
		if err != api.DB_SUCCESS {
			return err
		}
		if c1 != "a" {
			return api.DB_SUCCESS
		}
		if err := api.TupleCopy(newTpl, oldTpl); err != api.DB_SUCCESS {
			return err
		}
		var c3 uint32
		if err := api.TupleReadU32(oldTpl, 2, &c3); err != api.DB_SUCCESS {
			return err
		}
		c3 += 100
		if err := api.TupleWriteU32(newTpl, 2, c3); err != api.DB_SUCCESS {
			return err
		}
		if err := api.CursorUpdateRow(crsr, oldTpl, newTpl); err != api.DB_SUCCESS {
			return err
		}
		if err := api.CursorNext(crsr); err != api.DB_SUCCESS {
			return err
		}
		oldTpl = api.TupleClear(oldTpl)
		newTpl = api.TupleClear(newTpl)
	}
}

func deleteRowBz(crsr *api.Cursor) api.ErrCode {
	if err := api.CursorSetMatchMode(crsr, api.IB_EXACT_MATCH); err != api.DB_SUCCESS {
		return err
	}
	key := api.SecSearchTupleCreate(crsr)
	if key == nil {
		return api.DB_ERROR
	}
	if err := api.ColSetValue(key, 0, []byte("b"), 1); err != api.DB_SUCCESS {
		api.TupleDelete(key)
		return err
	}
	if err := api.ColSetValue(key, 1, []byte("z"), 1); err != api.DB_SUCCESS {
		api.TupleDelete(key)
		return err
	}
	var res int
	err := api.CursorMoveTo(crsr, key, api.CursorGE, &res)
	api.TupleDelete(key)
	if err != api.DB_SUCCESS {
		return err
	}
	if res != 0 {
		return api.DB_ERROR
	}
	return api.CursorDeleteRow(crsr)
}

func scanTest1Rows(crsr *api.Cursor, fn func(*data.Tuple) api.ErrCode) (int, api.ErrCode) {
	if err := api.CursorFirst(crsr); err != api.DB_SUCCESS {
		if err == api.DB_RECORD_NOT_FOUND {
			return 0, api.DB_SUCCESS
		}
		return 0, err
	}
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
		if fn != nil {
			if err := fn(tpl); err != api.DB_SUCCESS {
				return count, err
			}
		}
		count++
		if err := api.CursorNext(crsr); err != api.DB_SUCCESS {
			if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
				return count, api.DB_SUCCESS
			}
			return count, err
		}
		tpl = api.TupleClear(tpl)
	}
}

func test1TableName() string {
	return test1DB + "/" + test1Table
}
