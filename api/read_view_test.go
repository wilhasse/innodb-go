package api

import "testing"

func TestCursorAssignsReadView(t *testing.T) {
	resetAPIState()
	if err := Init(); err != DB_SUCCESS {
		t.Fatalf("Init: %v", err)
	}
	defer func() {
		_ = Shutdown(ShutdownNormal)
	}()
	if err := Startup("barracuda"); err != DB_SUCCESS {
		t.Fatalf("Startup: %v", err)
	}
	if err := DatabaseCreate("read_view_db"); err != DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	var schema *TableSchema
	if err := TableSchemaCreate("read_view_db/t", &schema, IB_TBL_COMPACT, 0); err != DB_SUCCESS {
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

	ibTrx := TrxBegin(IB_TRX_REPEATABLE_READ)
	var crsr *Cursor
	if err := CursorOpenTable("read_view_db/t", ibTrx, &crsr); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable: %v", err)
	}
	if ibTrx.ReadView == nil {
		t.Fatalf("expected read view assigned on cursor open")
	}

	otherTrx := TrxBegin(IB_TRX_REPEATABLE_READ)
	var crsr2 *Cursor
	if err := CursorOpenTable("read_view_db/t", nil, &crsr2); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable nil trx: %v", err)
	}
	if err := CursorAttachTrx(crsr2, otherTrx); err != DB_SUCCESS {
		t.Fatalf("CursorAttachTrx: %v", err)
	}
	if otherTrx.ReadView == nil {
		t.Fatalf("expected read view assigned on attach")
	}
	if err := TrxRollback(ibTrx); err != DB_SUCCESS {
		t.Fatalf("TrxRollback: %v", err)
	}
	if err := TrxRollback(otherTrx); err != DB_SUCCESS {
		t.Fatalf("TrxRollback other: %v", err)
	}
}
