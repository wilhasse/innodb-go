package api

import "testing"

func TestConsistentReadUsesUndoVersion(t *testing.T) {
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
	if err := DatabaseCreate("cons_read_db"); err != DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	var schema *TableSchema
	if err := TableSchemaCreate("cons_read_db/t", &schema, IB_TBL_COMPACT, 0); err != DB_SUCCESS {
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
	if err := CursorOpenTable("cons_read_db/t", baseTrx, &baseCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable base: %v", err)
	}
	baseTpl := ClustReadTupleCreate(baseCur)
	if baseTpl == nil {
		t.Fatalf("ClustReadTupleCreate base returned nil")
	}
	if err := TupleWriteU32(baseTpl, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 base c1: %v", err)
	}
	if err := TupleWriteU32(baseTpl, 1, 100); err != DB_SUCCESS {
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
	if err := CursorOpenTable("cons_read_db/t", reader, &readCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable reader: %v", err)
	}
	if err := CursorSetMatchMode(readCur, IB_EXACT_MATCH); err != DB_SUCCESS {
		t.Fatalf("CursorSetMatchMode reader: %v", err)
	}
	if got := readValueByKey(t, readCur, 1); got != 100 {
		t.Fatalf("reader initial=%d want 100", got)
	}

	writer := TrxBegin(IB_TRX_REPEATABLE_READ)
	var writeCur *Cursor
	if err := CursorOpenTable("cons_read_db/t", writer, &writeCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable writer: %v", err)
	}
	oldTpl := ClustSearchTupleCreate(writeCur)
	if oldTpl == nil {
		t.Fatalf("ClustSearchTupleCreate old returned nil")
	}
	if err := TupleWriteU32(oldTpl, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 old: %v", err)
	}
	newTpl := ClustSearchTupleCreate(writeCur)
	if newTpl == nil {
		t.Fatalf("ClustSearchTupleCreate new returned nil")
	}
	if err := TupleWriteU32(newTpl, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 new c1: %v", err)
	}
	if err := TupleWriteU32(newTpl, 1, 200); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 new c2: %v", err)
	}
	if err := CursorUpdateRow(writeCur, oldTpl, newTpl); err != DB_SUCCESS {
		t.Fatalf("CursorUpdateRow: %v", err)
	}
	if err := TrxCommit(writer); err != DB_SUCCESS {
		t.Fatalf("TrxCommit writer: %v", err)
	}

	if got := readValueByKey(t, readCur, 1); got != 100 {
		t.Fatalf("reader after update=%d want 100", got)
	}
	if err := TrxRollback(reader); err != DB_SUCCESS {
		t.Fatalf("TrxRollback reader: %v", err)
	}
}

func readValueByKey(t *testing.T, crsr *Cursor, key uint32) uint32 {
	t.Helper()
	if err := CursorReset(crsr); err != DB_SUCCESS {
		t.Fatalf("CursorReset: %v", err)
	}
	search := ClustSearchTupleCreate(crsr)
	if search == nil {
		t.Fatalf("ClustSearchTupleCreate search returned nil")
	}
	if err := TupleWriteU32(search, 0, key); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 search: %v", err)
	}
	var ret int
	if err := CursorMoveTo(crsr, search, CursorGE, &ret); err != DB_SUCCESS {
		t.Fatalf("CursorMoveTo: %v", err)
	}
	readTpl := ClustReadTupleCreate(crsr)
	if readTpl == nil {
		t.Fatalf("ClustReadTupleCreate read returned nil")
	}
	if err := CursorReadRow(crsr, readTpl); err != DB_SUCCESS {
		t.Fatalf("CursorReadRow: %v", err)
	}
	var got uint32
	if err := TupleReadU32(readTpl, 1, &got); err != DB_SUCCESS {
		t.Fatalf("TupleReadU32: %v", err)
	}
	return got
}

func TestConsistentReadSecondaryIndex(t *testing.T) {
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
	if err := DatabaseCreate("cons_read_sec_db"); err != DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	var schema *TableSchema
	if err := TableSchemaCreate("cons_read_sec_db/t", &schema, IB_TBL_COMPACT, 0); err != DB_SUCCESS {
		t.Fatalf("TableSchemaCreate: %v", err)
	}
	if err := TableSchemaAddCol(schema, "c1", IB_INT, IB_COL_UNSIGNED, 0, 4); err != DB_SUCCESS {
		t.Fatalf("TableSchemaAddCol c1: %v", err)
	}
	if err := TableSchemaAddCol(schema, "c2", IB_INT, IB_COL_UNSIGNED, 0, 4); err != DB_SUCCESS {
		t.Fatalf("TableSchemaAddCol c2: %v", err)
	}
	if err := TableSchemaAddCol(schema, "c3", IB_INT, IB_COL_UNSIGNED, 0, 4); err != DB_SUCCESS {
		t.Fatalf("TableSchemaAddCol c3: %v", err)
	}
	var pk *IndexSchema
	if err := TableSchemaAddIndex(schema, "PRIMARY", &pk); err != DB_SUCCESS {
		t.Fatalf("TableSchemaAddIndex: %v", err)
	}
	if err := IndexSchemaAddCol(pk, "c1", 0); err != DB_SUCCESS {
		t.Fatalf("IndexSchemaAddCol: %v", err)
	}
	if err := IndexSchemaSetClustered(pk); err != DB_SUCCESS {
		t.Fatalf("IndexSchemaSetClustered: %v", err)
	}
	if err := TableCreate(nil, schema, nil); err != DB_SUCCESS {
		t.Fatalf("TableCreate: %v", err)
	}

	var sec *IndexSchema
	if err := IndexSchemaCreate(nil, "idx_c2", "cons_read_sec_db/t", &sec); err != DB_SUCCESS {
		t.Fatalf("IndexSchemaCreate: %v", err)
	}
	if err := IndexSchemaAddCol(sec, "c2", 0); err != DB_SUCCESS {
		t.Fatalf("IndexSchemaAddCol sec: %v", err)
	}
	if err := IndexCreate(sec, nil); err != DB_SUCCESS {
		t.Fatalf("IndexCreate: %v", err)
	}

	baseTrx := TrxBegin(IB_TRX_REPEATABLE_READ)
	var baseCur *Cursor
	if err := CursorOpenTable("cons_read_sec_db/t", baseTrx, &baseCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable base: %v", err)
	}
	baseTpl := ClustReadTupleCreate(baseCur)
	if baseTpl == nil {
		t.Fatalf("ClustReadTupleCreate base returned nil")
	}
	if err := TupleWriteU32(baseTpl, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 base c1: %v", err)
	}
	if err := TupleWriteU32(baseTpl, 1, 100); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 base c2: %v", err)
	}
	if err := TupleWriteU32(baseTpl, 2, 10); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 base c3: %v", err)
	}
	if err := CursorInsertRow(baseCur, baseTpl); err != DB_SUCCESS {
		t.Fatalf("CursorInsertRow base: %v", err)
	}
	if err := TrxCommit(baseTrx); err != DB_SUCCESS {
		t.Fatalf("TrxCommit base: %v", err)
	}

	reader := TrxBegin(IB_TRX_REPEATABLE_READ)
	var readCur *Cursor
	if err := CursorOpenTable("cons_read_sec_db/t", reader, &readCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable reader: %v", err)
	}
	var secCur *Cursor
	if err := CursorOpenIndexUsingName(readCur, "idx_c2", &secCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenIndexUsingName: %v", err)
	}
	if err := CursorSetMatchMode(secCur, IB_EXACT_MATCH); err != DB_SUCCESS {
		t.Fatalf("CursorSetMatchMode sec: %v", err)
	}
	if got := readC3BySecondary(t, secCur, 100); got != 10 {
		t.Fatalf("reader initial=%d want 10", got)
	}

	writer := TrxBegin(IB_TRX_REPEATABLE_READ)
	var writeCur *Cursor
	if err := CursorOpenTable("cons_read_sec_db/t", writer, &writeCur); err != DB_SUCCESS {
		t.Fatalf("CursorOpenTable writer: %v", err)
	}
	oldTpl := ClustSearchTupleCreate(writeCur)
	if oldTpl == nil {
		t.Fatalf("ClustSearchTupleCreate old returned nil")
	}
	if err := TupleWriteU32(oldTpl, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 old: %v", err)
	}
	newTpl := ClustSearchTupleCreate(writeCur)
	if newTpl == nil {
		t.Fatalf("ClustSearchTupleCreate new returned nil")
	}
	if err := TupleWriteU32(newTpl, 0, 1); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 new c1: %v", err)
	}
	if err := TupleWriteU32(newTpl, 1, 100); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 new c2: %v", err)
	}
	if err := TupleWriteU32(newTpl, 2, 20); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 new c3: %v", err)
	}
	if err := CursorUpdateRow(writeCur, oldTpl, newTpl); err != DB_SUCCESS {
		t.Fatalf("CursorUpdateRow: %v", err)
	}
	if err := TrxCommit(writer); err != DB_SUCCESS {
		t.Fatalf("TrxCommit writer: %v", err)
	}

	if got := readC3BySecondary(t, secCur, 100); got != 10 {
		t.Fatalf("reader after update=%d want 10", got)
	}
	if err := TrxRollback(reader); err != DB_SUCCESS {
		t.Fatalf("TrxRollback reader: %v", err)
	}
}

func readC3BySecondary(t *testing.T, crsr *Cursor, key uint32) uint32 {
	t.Helper()
	if err := CursorReset(crsr); err != DB_SUCCESS {
		t.Fatalf("CursorReset: %v", err)
	}
	search := SecSearchTupleCreate(crsr)
	if search == nil {
		t.Fatalf("SecSearchTupleCreate search returned nil")
	}
	if err := TupleWriteU32(search, 0, 0); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 search c1: %v", err)
	}
	if err := TupleWriteU32(search, 1, key); err != DB_SUCCESS {
		t.Fatalf("TupleWriteU32 search c2: %v", err)
	}
	var ret int
	if err := CursorMoveTo(crsr, search, CursorGE, &ret); err != DB_SUCCESS {
		t.Fatalf("CursorMoveTo: %v", err)
	}
	readTpl := ClustReadTupleCreate(crsr)
	if readTpl == nil {
		t.Fatalf("ClustReadTupleCreate read returned nil")
	}
	if err := CursorReadRow(crsr, readTpl); err != DB_SUCCESS {
		t.Fatalf("CursorReadRow: %v", err)
	}
	var got uint32
	if err := TupleReadU32(readTpl, 2, &got); err != DB_SUCCESS {
		t.Fatalf("TupleReadU32: %v", err)
	}
	return got
}
