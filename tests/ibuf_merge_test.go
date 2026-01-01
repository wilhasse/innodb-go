package tests

import (
	"fmt"
	"testing"

	"github.com/wilhasse/innodb-go/api"
	"github.com/wilhasse/innodb-go/ibuf"
)

func TestInsertBufferMergeOnIndexRead(t *testing.T) {
	resetAPI(t)
	ibuf.InitAtDBStart()
	ibuf.Use = ibuf.UseInsert
	if err := api.Init(); err != api.DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	defer func() {
		_ = api.Shutdown(api.ShutdownNormal)
	}()
	if err := api.Startup("barracuda"); err != api.DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}
	if err := api.DatabaseCreate("ibuf_db"); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	tableName := "ibuf_db/t"
	if err := createRestartIndexTable(tableName); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}
	if err := createSecondaryIndex(tableName, "c2", 0); err != api.DB_SUCCESS {
		t.Fatalf("create index: %v", err)
	}

	if err := insertRestartIndexRows(tableName); err != api.DB_SUCCESS {
		t.Fatalf("insert rows: %v", err)
	}
	if got := ibuf.Count(); got != 4 {
		t.Fatalf("ibuf count=%d want %d", got, 4)
	}

	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, trx, &crsr); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		t.Fatalf("CursorOpenTable: %v", err)
	}
	indexName := fmt.Sprintf("%s_%s", tableName, "c2")
	var idxCrsr *api.Cursor
	if err := api.CursorOpenIndexUsingName(crsr, indexName, &idxCrsr); err != api.DB_SUCCESS {
		_ = api.CursorClose(crsr)
		_ = api.TrxRollback(trx)
		t.Fatalf("CursorOpenIndexUsingName: %v", err)
	}
	if err := assertSecondaryIndexOrder(idxCrsr); err != api.DB_SUCCESS {
		_ = api.CursorClose(idxCrsr)
		_ = api.CursorClose(crsr)
		_ = api.TrxRollback(trx)
		t.Fatalf("assertSecondaryIndexOrder: %v", err)
	}
	if got := ibuf.Count(); got != 0 {
		t.Fatalf("ibuf count after merge=%d want 0", got)
	}
	_ = api.CursorClose(idxCrsr)
	_ = api.CursorClose(crsr)
	if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
		t.Fatalf("TrxCommit: %v", err)
	}

	if err := api.TableDrop(nil, tableName); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop("ibuf_db"); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}
