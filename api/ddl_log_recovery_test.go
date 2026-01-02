package api

import (
	"path/filepath"
	"testing"

	"github.com/wilhasse/innodb-go/dict"
	ibos "github.com/wilhasse/innodb-go/os"
)

func TestDDLLogDropRecovery(t *testing.T) {
	resetAPIState()

	dir := t.TempDir()
	dataDir := dir + "/"
	dataFilePath := "ibdata1:4M:autoextend"
	tableName := "ddl_log_db/t1"

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
	if err := DatabaseCreate("ddl_log_db"); err != DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	if err := createDDLLogTable(tableName); err != DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}

	if err := writeDDLLogEntry(ddlLogEntry{Op: ddlOpDrop, Table: tableName}); err != nil {
		t.Fatalf("write ddl log: %v", err)
	}
	if err := Shutdown(ShutdownNormal); err != DB_SUCCESS {
		t.Fatalf("Shutdown: %v", err)
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
	if dict.DictTableGet(tableName) != nil {
		t.Fatalf("expected table dropped after recovery")
	}
	if exists, _ := ibos.FileExists(ddlLogPath()); exists {
		t.Fatalf("expected ddl log cleared")
	}
	if filePerTableEnabled() {
		if path, err := tableFilePath(tableName); err == DB_SUCCESS {
			if exists, _ := ibos.FileExists(path); exists {
				t.Fatalf("expected table file removed")
			}
		}
	}
	if err := Shutdown(ShutdownNormal); err != DB_SUCCESS {
		t.Fatalf("Shutdown restart: %v", err)
	}
}

func TestDDLLogRenameRecovery(t *testing.T) {
	resetAPIState()

	dir := t.TempDir()
	dataDir := dir + "/"
	dataFilePath := "ibdata1:4M:autoextend"
	oldName := "ddl_log_db/t_old"
	newName := "ddl_log_db/t_new"

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
	if err := DatabaseCreate("ddl_log_db"); err != DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	if err := createDDLLogTable(oldName); err != DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}
	oldPath := ""
	newPath := ""
	if filePerTableEnabled() {
		if path, err := tableFilePath(oldName); err == DB_SUCCESS {
			oldPath = path
		}
		if path, err := tableFilePath(newName); err == DB_SUCCESS {
			newPath = path
		}
	}

	if err := writeDDLLogEntry(ddlLogEntry{Op: ddlOpRename, Table: oldName, NewTable: newName}); err != nil {
		t.Fatalf("write ddl log: %v", err)
	}
	if err := Shutdown(ShutdownNormal); err != DB_SUCCESS {
		t.Fatalf("Shutdown: %v", err)
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
	if dict.DictTableGet(oldName) != nil {
		t.Fatalf("expected old name removed after recovery")
	}
	if dict.DictTableGet(newName) == nil {
		t.Fatalf("expected renamed table after recovery")
	}
	if filePerTableEnabled() && oldPath != "" && newPath != "" {
		if exists, _ := ibos.FileExists(filepath.Clean(oldPath)); exists {
			t.Fatalf("expected old table file removed")
		}
		if exists, _ := ibos.FileExists(filepath.Clean(newPath)); !exists {
			t.Fatalf("expected renamed table file")
		}
	}
	if err := Shutdown(ShutdownNormal); err != DB_SUCCESS {
		t.Fatalf("Shutdown restart: %v", err)
	}
}

func createDDLLogTable(name string) ErrCode {
	var schema *TableSchema
	if err := TableSchemaCreate(name, &schema, IB_TBL_COMPACT, 0); err != DB_SUCCESS {
		return err
	}
	if err := TableSchemaAddCol(schema, "c1", IB_INT, IB_COL_UNSIGNED, 0, 4); err != DB_SUCCESS {
		return err
	}
	var idx *IndexSchema
	if err := TableSchemaAddIndex(schema, "PRIMARY", &idx); err != DB_SUCCESS {
		return err
	}
	if err := IndexSchemaAddCol(idx, "c1", 0); err != DB_SUCCESS {
		return err
	}
	if err := IndexSchemaSetClustered(idx); err != DB_SUCCESS {
		return err
	}
	return TableCreate(nil, schema, nil)
}
