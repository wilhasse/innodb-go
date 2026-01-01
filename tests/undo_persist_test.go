package tests

import (
	"testing"

	"github.com/wilhasse/innodb-go/api"
	"github.com/wilhasse/innodb-go/trx"
)

func TestUndoPersistenceReload(t *testing.T) {
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
	if err := api.DatabaseCreate("undo_persist"); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	tableName := "undo_persist/t"
	if err := createRestartIndexTable(tableName); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}
	if err := insertRestartIndexRows(tableName); err != api.DB_SUCCESS {
		t.Fatalf("insert rows: %v", err)
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
	if trx.UndoRecoveredCount() == 0 {
		t.Fatalf("expected recovered undo records after restart")
	}
	if err := api.TableDrop(nil, tableName); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop("undo_persist"); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
	if err := api.Shutdown(api.ShutdownNormal); err != api.DB_SUCCESS {
		t.Fatalf("Shutdown final: %v", err)
	}
}
