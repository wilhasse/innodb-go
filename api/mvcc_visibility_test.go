package api

import "testing"

func TestConsistentReadInsertInvisible(t *testing.T) {
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
	if err := DatabaseCreate("mvcc_insert_db"); err != DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	var schema *TableSchema
	if err := TableSchemaCreate("mvcc_insert_db/t", &schema, IB_TBL_COMPACT, 0); err != DB_SUCCESS {
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

	reader := TrxBegin(IB_TRX_REPEATABLE_READ)
	var readCur *Cursor
	if err := CursorOpenTable("mvcc_insert_db/t", reader, &readCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable reader: %v", err)
	}
	if err := CursorSetMatchMode(readCur, IB_EXACT_MATCH); err != DB_SUCCESS {
		t.Fatalf("CursorSetMatchMode reader: %v", err)
	}

	writer := TrxBegin(IB_TRX_REPEATABLE_READ)
	var writeCur *Cursor
	if err := CursorOpenTable("mvcc_insert_db/t", writer, &writeCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable writer: %v", err)
	}
	insertTpl := ClustReadTupleCreate(writeCur)
	if insertTpl == nil {
		t.Fatalf("ClustReadTupleCreate insert returned nil")
	}
	if err := TupleWriteU32(insertTpl, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 insert c1: %v", err)
	}
	if err := TupleWriteU32(insertTpl, 1, 111); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 insert c2: %v", err)
	}
	if err := CursorInsertRow(writeCur, insertTpl); err != DB_SUCCESS {
		t.Fatalf("CursorInsertRow: %v", err)
	}
	if err := TrxCommit(writer); err != DB_SUCCESS {
		t.Fatalf("TrxCommit writer: %v", err)
	}

	search := ClustSearchTupleCreate(readCur)
	if search == nil {
		t.Fatalf("ClustSearchTupleCreate search returned nil")
	}
	if err := TupleWriteU32(search, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 search: %v", err)
	}
	var ret int
	err := CursorMoveTo(readCur, search, CursorGE, &ret)
	if err == DB_SUCCESS {
		readTpl := ClustReadTupleCreate(readCur)
		if readTpl == nil {
			t.Fatalf("ClustReadTupleCreate read returned nil")
		}
		if got := CursorReadRow(readCur, readTpl); got != DB_RECORD_NOT_FOUND {
			t.Fatalf("expected insert invisible, got %v", got)
		}
	} else if err != DB_RECORD_NOT_FOUND {
		t.Fatalf("CursorMoveTo: %v", err)
	}
	if err := TrxRollback(reader); err != DB_SUCCESS {
		t.Fatalf("TrxRollback reader: %v", err)
	}
}

func TestConsistentReadDeleteVisible(t *testing.T) {
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
	if err := DatabaseCreate("mvcc_delete_db"); err != DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	var schema *TableSchema
	if err := TableSchemaCreate("mvcc_delete_db/t", &schema, IB_TBL_COMPACT, 0); err != DB_SUCCESS {
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

	baseTrx := TrxBegin(IB_TRX_REPEATABLE_READ)
	var baseCur *Cursor
	if err := CursorOpenTable("mvcc_delete_db/t", baseTrx, &baseCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable base: %v", err)
	}
	baseTpl := ClustReadTupleCreate(baseCur)
	if baseTpl == nil {
		t.Fatalf("ClustReadTupleCreate base returned nil")
	}
	if err := TupleWriteU32(baseTpl, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 base c1: %v", err)
	}
	if err := TupleWriteU32(baseTpl, 1, 222); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 base c2: %v", err)
	}
	if err := CursorInsertRow(baseCur, baseTpl); err != DB_SUCCESS {
		t.Fatalf("CursorInsertRow base: %v", err)
	}
	if err := TrxCommit(baseTrx); err != DB_SUCCESS {
		t.Fatalf("TrxCommit base: %v", err)
	}

	reader := TrxBegin(IB_TRX_REPEATABLE_READ)
	var readCur *Cursor
	if err := CursorOpenTable("mvcc_delete_db/t", reader, &readCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable reader: %v", err)
	}
	if err := CursorSetMatchMode(readCur, IB_EXACT_MATCH); err != DB_SUCCESS {
		t.Fatalf("CursorSetMatchMode reader: %v", err)
	}

	writer := TrxBegin(IB_TRX_REPEATABLE_READ)
	var writeCur *Cursor
	if err := CursorOpenTable("mvcc_delete_db/t", writer, &writeCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable writer: %v", err)
	}
	search := ClustSearchTupleCreate(writeCur)
	if search == nil {
		t.Fatalf("ClustSearchTupleCreate delete search returned nil")
	}
	if err := TupleWriteU32(search, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 delete search: %v", err)
	}
	var ret int
	if err := CursorMoveTo(writeCur, search, CursorGE, &ret); err != DB_SUCCESS {
		t.Fatalf("CursorMoveTo delete: %v", err)
	}
	if err := CursorDeleteRow(writeCur); err != DB_SUCCESS {
		t.Fatalf("CursorDeleteRow: %v", err)
	}
	if err := TrxCommit(writer); err != DB_SUCCESS {
		t.Fatalf("TrxCommit writer: %v", err)
	}

	if got := readValueByKey(t, readCur, 1); got != 222 {
		t.Fatalf("reader after delete=%d want 222", got)
	}
	if err := TrxRollback(reader); err != DB_SUCCESS {
		t.Fatalf("TrxRollback reader: %v", err)
	}
}
