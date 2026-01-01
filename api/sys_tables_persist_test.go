package api

import (
	"path/filepath"
	"testing"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/dict"
	ibos "github.com/wilhasse/innodb-go/os"
)

func TestSysTablesBtrPersistence(t *testing.T) {
	resetAPIState()

	dir := t.TempDir()
	dataDir := dir + "/"
	dataFilePath := "ibdata1:4M:autoextend"

	if err := Init(); err != DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	if err := CfgSet("data_home_dir", dataDir); err != DB_SUCCESS {
		t.Fatalf("CfgSet data_home_dir: %v", err)
	}
	if err := CfgSet("data_file_path", dataFilePath); err != DB_SUCCESS {
		t.Fatalf("CfgSet data_file_path: %v", err)
	}
	if err := Startup(""); err != DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}

	if err := DatabaseCreate("sys_persist_db"); err != DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	var schema *TableSchema
	if err := TableSchemaCreate("sys_persist_db/t", &schema, IB_TBL_COMPACT, 0); err != DB_SUCCESS {
		t.Fatalf("TableSchemaCreate: %v", err)
	}
	if err := TableSchemaAddCol(schema, "c1", IB_INT, IB_COL_UNSIGNED, 0, 4); err != DB_SUCCESS {
		t.Fatalf("TableSchemaAddCol: %v", err)
	}
	var idx *IndexSchema
	if err := TableSchemaAddIndex(schema, "PRIMARY", &idx); err != DB_SUCCESS {
		t.Fatalf("TableSchemaAddIndex: %v", err)
	}
	if err := IndexSchemaAddCol(idx, "c1", 0); err != DB_SUCCESS {
		t.Fatalf("IndexSchemaAddCol: %v", err)
	}
	if err := IndexSchemaSetClustered(idx); err != DB_SUCCESS {
		t.Fatalf("IndexSchemaSetClustered: %v", err)
	}
	if err := TableCreate(nil, schema, nil); err != DB_SUCCESS {
		t.Fatalf("TableCreate: %v", err)
	}
	persistRows, err := (&sysTablePersister{}).LoadSysRows()
	if err != nil {
		t.Fatalf("LoadSysRows: %v", err)
	}
	if !sysRowsHaveTable(persistRows.Tables, "sys_persist_db/t") {
		t.Fatalf("expected SYS_TABLES row after create")
	}
	if err := Shutdown(ShutdownNormal); err != DB_SUCCESS {
		t.Fatalf("Shutdown: %v", err)
	}

	if exists, _ := ibos.FileExists(filepath.Join(dataDir, "ib_dict.sys")); !exists {
		t.Fatalf("expected dict file header")
	}

	if err := Init(); err != DB_SUCCESS {
		t.Fatalf("Init restart: %v", err)
	}
	if err := CfgSet("data_home_dir", dataDir); err != DB_SUCCESS {
		t.Fatalf("CfgSet data_home_dir restart: %v", err)
	}
	if err := CfgSet("data_file_path", dataFilePath); err != DB_SUCCESS {
		t.Fatalf("CfgSet data_file_path restart: %v", err)
	}
	if err := Startup(""); err != DB_SUCCESS {
		t.Fatalf("Startup restart: %v", err)
	}
	if dict.DictSys == nil || !sysRowsHaveTable(dict.DictSys.SysRows.Tables, "sys_persist_db/t") {
		t.Fatalf("expected dict sys rows after restart")
	}
	if dict.DictTableGet("sys_persist_db/t") == nil {
		t.Fatalf("expected dict table after restart; tables=%v", dict.DictListTables())
	}
	restartRows, err := (&sysTablePersister{}).LoadSysRows()
	if err != nil {
		t.Fatalf("LoadSysRows restart: %v", err)
	}
	if !sysRowsHaveTable(restartRows.Tables, "sys_persist_db/t") {
		t.Fatalf("expected SYS_TABLES row after restart")
	}
	trx := TrxBegin(IB_TRX_REPEATABLE_READ)
	var cur *Cursor
	if err := CursorOpenTable("sys_persist_db/t", trx, &cur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable: %v", err)
	}
	if err := TrxRollback(trx); err != DB_SUCCESS {
		t.Fatalf("TrxRollback: %v", err)
	}
	if err := Shutdown(ShutdownNormal); err != DB_SUCCESS {
		t.Fatalf("Shutdown restart: %v", err)
	}
}

func sysRowsHaveTable(rows []*data.Tuple, name string) bool {
	for _, row := range rows {
		if row == nil || len(row.Fields) == 0 {
			continue
		}
		if string(row.Fields[0].Data) == name {
			return true
		}
	}
	return false
}
