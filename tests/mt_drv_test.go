package tests

import (
	"fmt"
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

type dmlOpType int
type ddlOpType int

const (
	dmlSelect dmlOpType = iota
	dmlInsert
	dmlUpdate
	dmlDelete
	dmlMax
)

const (
	ddlCreate ddlOpType = iota
	ddlDrop
	ddlAlter
	ddlTruncate
	ddlMax
)

type opErr struct {
	nOps  int
	nErrs int
	errs  map[api.ErrCode]int
}

type tblClass struct {
	name   string
	dbName string
	dmlFn  [dmlMax]func(*cbArgs) api.ErrCode
	ddlFn  [ddlMax]func(*cbArgs) api.ErrCode
}

type cbArgs struct {
	rowID     uint32
	errSt     *opErr
	tbl       *tblClass
}

func TestMtDrvHarness(t *testing.T) {
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
	if err := api.DatabaseCreate("test"); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}

	table := tblClass{
		name:   "mt_tbl",
		dbName: "test",
	}
	table.ddlFn[ddlCreate] = mtCreateTable
	table.ddlFn[ddlDrop] = mtDropTable
	table.ddlFn[ddlTruncate] = mtTruncateTable
	table.dmlFn[dmlInsert] = mtInsertRow
	table.dmlFn[dmlSelect] = mtSelectStub

	ddlErrs := &opErr{errs: map[api.ErrCode]int{}}
	dmlErrs := &opErr{errs: map[api.ErrCode]int{}}

	args := cbArgs{tbl: &table, errSt: ddlErrs}
	err := table.ddlFn[ddlCreate](&args)
	updateErrStats(ddlErrs, err)
	if err != api.DB_SUCCESS {
		t.Fatalf("create: %v", err)
	}

	for i := uint32(1); i <= 5; i++ {
		args := cbArgs{tbl: &table, errSt: dmlErrs, rowID: i}
		err := table.dmlFn[dmlInsert](&args)
		updateErrStats(dmlErrs, err)
		if err != api.DB_SUCCESS {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}

	args = cbArgs{tbl: &table, errSt: dmlErrs}
	err = table.dmlFn[dmlSelect](&args)
	updateErrStats(dmlErrs, err)
	if err != api.DB_SUCCESS {
		t.Fatalf("select: %v", err)
	}

	args = cbArgs{tbl: &table, errSt: ddlErrs}
	err = table.ddlFn[ddlTruncate](&args)
	updateErrStats(ddlErrs, err)
	if err != api.DB_SUCCESS {
		t.Fatalf("truncate: %v", err)
	}

	args = cbArgs{tbl: &table, errSt: ddlErrs}
	err = table.ddlFn[ddlDrop](&args)
	updateErrStats(ddlErrs, err)
	if err != api.DB_SUCCESS {
		t.Fatalf("drop: %v", err)
	}

	if ddlErrs.nOps != 3 || ddlErrs.nErrs != 0 {
		t.Fatalf("ddl stats: %+v", ddlErrs)
	}
	if dmlErrs.nOps != 6 || dmlErrs.nErrs != 0 {
		t.Fatalf("dml stats: %+v", dmlErrs)
	}
}

func updateErrStats(e *opErr, err api.ErrCode) {
	if e == nil {
		return
	}
	e.nOps++
	if err != api.DB_SUCCESS {
		e.nErrs++
		if e.errs == nil {
			e.errs = map[api.ErrCode]int{}
		}
		e.errs[err]++
	}
}

func mtCreateTable(arg *cbArgs) api.ErrCode {
	if arg == nil || arg.tbl == nil {
		return api.DB_ERROR
	}
	tableName := fmt.Sprintf("%s/%s", arg.tbl.dbName, arg.tbl.name)
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
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

func mtDropTable(arg *cbArgs) api.ErrCode {
	if arg == nil || arg.tbl == nil {
		return api.DB_ERROR
	}
	tableName := fmt.Sprintf("%s/%s", arg.tbl.dbName, arg.tbl.name)
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if err := api.SchemaLockExclusive(trx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableDrop(trx, tableName); err != api.DB_SUCCESS && err != api.DB_TABLE_NOT_FOUND {
		_ = api.TrxRollback(trx)
		return err
	}
	return api.TrxCommit(trx)
}

func mtTruncateTable(arg *cbArgs) api.ErrCode {
	if arg == nil || arg.tbl == nil {
		return api.DB_ERROR
	}
	tableName := fmt.Sprintf("%s/%s", arg.tbl.dbName, arg.tbl.name)
	return api.TableTruncate(tableName, nil)
}

func mtInsertRow(arg *cbArgs) api.ErrCode {
	if arg == nil || arg.tbl == nil {
		return api.DB_ERROR
	}
	tableName := fmt.Sprintf("%s/%s", arg.tbl.dbName, arg.tbl.name)
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
	if err := api.TupleWriteU32(tpl, 0, arg.rowID); err != api.DB_SUCCESS {
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
	api.TupleDelete(tpl)
	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	return api.TrxCommit(trx)
}

func mtSelectStub(_ *cbArgs) api.ErrCode {
	return api.DB_SUCCESS
}
