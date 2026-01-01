package tests

import (
	"sort"
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	restartSchemaDB    = "restart_schema"
	restartSchemaTable = "t1"
)

func TestRestartSchemaPersistence(t *testing.T) {
	resetAPI(t)
	dir := t.TempDir() + "/"

	if err := api.Init(); err != api.DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	if err := api.CfgSet("data_home_dir", dir); err != api.DB_SUCCESS {
		t.Fatalf("CfgSet data_home_dir: %v", err)
	}
	if err := api.Startup("barracuda"); err != api.DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}
	if err := api.DatabaseCreate(restartSchemaDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}

	tableName := restartSchemaDB + "/" + restartSchemaTable
	if err := createRestartSchemaTable(tableName); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}

	if err := api.Shutdown(api.ShutdownNormal); err != api.DB_SUCCESS {
		t.Fatalf("Shutdown: %v", err)
	}

	if err := api.Init(); err != api.DB_SUCCESS {
		t.Fatalf("Init after restart: %v", err)
	}
	if err := api.CfgSet("data_home_dir", dir); err != api.DB_SUCCESS {
		t.Fatalf("CfgSet data_home_dir after restart: %v", err)
	}
	if err := api.Startup("barracuda"); err != api.DB_SUCCESS {
		t.Fatalf("Startup after restart: %v", err)
	}

	trx := api.TrxBegin(api.IB_TRX_SERIALIZABLE)
	if err := api.SchemaLockExclusive(trx); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		t.Fatalf("SchemaLockExclusive: %v", err)
	}

	var tables []string
	if err := api.SchemaTablesIterate(trx, func(_ any, name string, _ int) int {
		tables = append(tables, name)
		return 0
	}, nil); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		t.Fatalf("SchemaTablesIterate: %v", err)
	}
	sort.Strings(tables)
	found := false
	for _, name := range tables {
		if name == tableName {
			found = true
			break
		}
	}
	if !found {
		_ = api.TrxRollback(trx)
		t.Fatalf("expected table %s after restart", tableName)
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
		t.Fatalf("TableSchemaVisit: %v", err)
	}
	sort.Strings(cols)
	sort.Strings(idxNames)
	if len(cols) != 2 || cols[0] != "c1" || cols[1] != "c2" {
		_ = api.TrxRollback(trx)
		t.Fatalf("unexpected columns: %v", cols)
	}
	if len(idxNames) != 2 || idxNames[0] != "PRIMARY" || idxNames[1] != "idx_c2" {
		_ = api.TrxRollback(trx)
		t.Fatalf("unexpected indexes: %v", idxNames)
	}
	if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
		t.Fatalf("TrxCommit: %v", err)
	}

	if err := api.TableDrop(nil, tableName); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(restartSchemaDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
	if err := api.Shutdown(api.ShutdownNormal); err != api.DB_SUCCESS {
		t.Fatalf("Shutdown final: %v", err)
	}
}

func createRestartSchemaTable(tableName string) api.ErrCode {
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c2", api.IB_VARCHAR, api.IB_COL_NONE, 0, 10); err != api.DB_SUCCESS {
		return err
	}
	var primary *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "PRIMARY", &primary); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(primary, "c1", 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaSetClustered(primary); err != api.DB_SUCCESS {
		return err
	}
	var secondary *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "idx_c2", &secondary); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(secondary, "c2", 0); err != api.DB_SUCCESS {
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
