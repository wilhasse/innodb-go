package api

import "testing"

func TestConsistentScanSkipsNewRows(t *testing.T) {
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
	if err := DatabaseCreate("mvcc_scan_db"); err != DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}

	var schema *TableSchema
	if err := TableSchemaCreate("mvcc_scan_db/t", &schema, IB_TBL_COMPACT, 0); err != DB_SUCCESS {
		t.Fatalf("TableSchemaCreate: %v", err)
	}
	if err := TableSchemaAddCol(schema, "c1", IB_INT, IB_COL_UNSIGNED, 0, 4); err != DB_SUCCESS {
		t.Fatalf("TableSchemaAddCol c1: %v", err)
	}
	if err := TableSchemaAddCol(schema, "c2", IB_INT, IB_COL_UNSIGNED, 0, 4); err != DB_SUCCESS {
		t.Fatalf("TableSchemaAddCol c2: %v", err)
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

	base := TrxBegin(IB_TRX_REPEATABLE_READ)
	var baseCur *Cursor
	if err := CursorOpenTable("mvcc_scan_db/t", base, &baseCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable base: %v", err)
	}
	if err := insertU32Row(baseCur, 1, 100); err != DB_SUCCESS {
		t.Fatalf("insert base 1: %v", err)
	}
	if err := insertU32Row(baseCur, 2, 200); err != DB_SUCCESS {
		t.Fatalf("insert base 2: %v", err)
	}
	if err := insertU32Row(baseCur, 3, 300); err != DB_SUCCESS {
		t.Fatalf("insert base 3: %v", err)
	}
	if err := TrxCommit(base); err != DB_SUCCESS {
		t.Fatalf("TrxCommit base: %v", err)
	}

	reader := TrxBegin(IB_TRX_REPEATABLE_READ)
	var readCur *Cursor
	if err := CursorOpenTable("mvcc_scan_db/t", reader, &readCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable reader: %v", err)
	}

	writer := TrxBegin(IB_TRX_REPEATABLE_READ)
	var writeCur *Cursor
	if err := CursorOpenTable("mvcc_scan_db/t", writer, &writeCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable writer: %v", err)
	}
	if err := insertU32Row(writeCur, 4, 400); err != DB_SUCCESS {
		t.Fatalf("insert writer 4: %v", err)
	}
	oldTpl := ClustSearchTupleCreate(writeCur)
	if oldTpl == nil {
		t.Fatalf("ClustSearchTupleCreate old returned nil")
	}
	if err := TupleWriteU32(oldTpl, 0, 2); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 old: %v", err)
	}
	newTpl := ClustSearchTupleCreate(writeCur)
	if newTpl == nil {
		t.Fatalf("ClustSearchTupleCreate new returned nil")
	}
	if err := TupleWriteU32(newTpl, 0, 2); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 new c1: %v", err)
	}
	if err := TupleWriteU32(newTpl, 1, 250); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 new c2: %v", err)
	}
	if err := CursorUpdateRow(writeCur, oldTpl, newTpl); err != DB_SUCCESS {
		t.Fatalf("CursorUpdateRow: %v", err)
	}
	if err := TrxCommit(writer); err != DB_SUCCESS {
		t.Fatalf("TrxCommit writer: %v", err)
	}

	keys, values, err := scanU32Rows(readCur)
	if err != DB_SUCCESS {
		t.Fatalf("scanU32Rows: %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("scan key count=%d want 3", len(keys))
	}
	if keys[0] != 1 || keys[1] != 2 || keys[2] != 3 {
		t.Fatalf("scan keys=%v want [1 2 3]", keys)
	}
	if values[0] != 100 || values[1] != 200 || values[2] != 300 {
		t.Fatalf("scan values=%v want [100 200 300]", values)
	}
	if err := TrxRollback(reader); err != DB_SUCCESS {
		t.Fatalf("TrxRollback reader: %v", err)
	}
}

func insertU32Row(crsr *Cursor, key, val uint32) ErrCode {
	tpl := ClustReadTupleCreate(crsr)
	if tpl == nil {
		return DB_ERROR
	}
	defer TupleDelete(tpl)
	if err := TupleWriteU32(tpl, 0, key); err != DB_SUCCESS {
		return err
	}
	if err := TupleWriteU32(tpl, 1, val); err != DB_SUCCESS {
		return err
	}
	return CursorInsertRow(crsr, tpl)
}

func scanU32Rows(crsr *Cursor) ([]uint32, []uint32, ErrCode) {
	if crsr == nil {
		return nil, nil, DB_ERROR
	}
	if err := CursorFirst(crsr); err != DB_SUCCESS {
		if err == DB_RECORD_NOT_FOUND || err == DB_END_OF_INDEX {
			return nil, nil, DB_SUCCESS
		}
		return nil, nil, err
	}
	readTpl := ClustReadTupleCreate(crsr)
	if readTpl == nil {
		return nil, nil, DB_ERROR
	}
	defer TupleDelete(readTpl)
	keys := make([]uint32, 0)
	values := make([]uint32, 0)
	for {
		if err := CursorReadRow(crsr, readTpl); err != DB_SUCCESS {
			return keys, values, err
		}
		var key uint32
		if err := TupleReadU32(readTpl, 0, &key); err != DB_SUCCESS {
			return keys, values, err
		}
		var val uint32
		if err := TupleReadU32(readTpl, 1, &val); err != DB_SUCCESS {
			return keys, values, err
		}
		keys = append(keys, key)
		values = append(values, val)
		err := CursorNext(crsr)
		if err == DB_END_OF_INDEX || err == DB_RECORD_NOT_FOUND {
			break
		}
		if err != DB_SUCCESS {
			return keys, values, err
		}
		readTpl = TupleClear(readTpl)
	}
	return keys, values, DB_SUCCESS
}
