package tests

import (
	"fmt"
	"sort"
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	dictDB    = "dict_test"
	dictTable = "t"
)

func TestDictHarness(t *testing.T) {
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

	if err := visitSysTables(); err != api.DB_SUCCESS {
		t.Fatalf("visitSysTables: %v", err)
	}

	if err := api.DatabaseCreate(dictDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := createDictTable(i); err != api.DB_SUCCESS {
			t.Fatalf("create table %d: %v", i, err)
		}
	}

	if err := verifySchemaIterate(); err != api.DB_SUCCESS {
		t.Fatalf("verifySchemaIterate: %v", err)
	}

	for i := 0; i < 10; i++ {
		if err := dropDictTable(i); err != api.DB_SUCCESS {
			t.Fatalf("drop table %d: %v", i, err)
		}
	}
	if err := api.DatabaseDrop(dictDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func visitSysTables() api.ErrCode {
	trx := api.TrxBegin(api.IB_TRX_SERIALIZABLE)
	if err := api.TableSchemaVisit(trx, "SYS_TABLES", &api.SchemaVisitor{}, nil); err != api.DB_SCHEMA_NOT_LOCKED {
		return err
	}
	if err := api.SchemaLockExclusive(trx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaVisit(trx, "SYS_TABLES", &api.SchemaVisitor{}, nil); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaVisit(trx, "SYS_COLUMNS", &api.SchemaVisitor{}, nil); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaVisit(trx, "SYS_INDEXES", &api.SchemaVisitor{}, nil); err != api.DB_SUCCESS {
		return err
	}
	return api.TrxCommit(trx)
}

func createDictTable(n int) api.ErrCode {
	tableName := fmt.Sprintf("%s/%s%d", dictDB, dictTable, n)
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "C1", api.IB_VARCHAR, api.IB_COL_NOT_NULL, 0, 10); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "C2", api.IB_VARCHAR, api.IB_COL_NOT_NULL, 0, 10); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "C3", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	var idx *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "PRIMARY", &idx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "C1", 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "C2", 0); err != api.DB_SUCCESS {
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

func dropDictTable(n int) api.ErrCode {
	tableName := fmt.Sprintf("%s/%s%d", dictDB, dictTable, n)
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	if err := api.SchemaLockExclusive(trx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableDrop(trx, tableName); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return err
	}
	return api.TrxCommit(trx)
}

func verifySchemaIterate() api.ErrCode {
	trx := api.TrxBegin(api.IB_TRX_SERIALIZABLE)
	if err := api.SchemaLockExclusive(trx); err != api.DB_SUCCESS {
		return err
	}
	var names []string
	iterErr := api.SchemaTablesIterate(trx, func(_ any, name string, _ int) int {
		names = append(names, name)
		return 0
	}, nil)
	if iterErr != api.DB_SUCCESS {
		return iterErr
	}
	sort.Strings(names)
	if len(names) != 10 {
		return api.DB_ERROR
	}

	tableName := fmt.Sprintf("%s/%s0", dictDB, dictTable)
	var gotCols []string
	var gotIdxCols []string
	var tableCols, tableIdx int
	visitor := api.SchemaVisitor{
		Mode: api.SchemaVisitorTableAndIndexCol,
		VisitTable: func(_ any, _ string, _ api.TableFormat, _ api.Ulint, nCols int, nIndexes int) int {
			tableCols = nCols
			tableIdx = nIndexes
			return 0
		},
		VisitColumn: func(_ any, name string, _ api.ColType, _ api.Ulint, _ api.ColAttr) int {
			gotCols = append(gotCols, name)
			return 0
		},
		VisitIndex: func(_ any, _ string, _ api.Bool, _ api.Bool, _ int) int {
			return 0
		},
		VisitIndexColumn: func(_ any, name string, _ api.Ulint) int {
			gotIdxCols = append(gotIdxCols, name)
			return 0
		},
	}
	if err := api.TableSchemaVisit(trx, tableName, &visitor, nil); err != api.DB_SUCCESS {
		return err
	}
	if tableCols != 3 || tableIdx != 1 {
		return api.DB_ERROR
	}
	if len(gotCols) != 3 || gotCols[0] != "C1" || gotCols[1] != "C2" || gotCols[2] != "C3" {
		return api.DB_ERROR
	}
	if len(gotIdxCols) != 2 || gotIdxCols[0] != "C1" || gotIdxCols[1] != "C2" {
		return api.DB_ERROR
	}
	return api.TrxCommit(trx)
}
