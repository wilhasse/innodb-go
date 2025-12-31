package tests

import (
	"testing"

	"github.com/wilhasse/innodb-go/api"
	"github.com/wilhasse/innodb-go/data"
)

const detailedDefaultLimit = 10

type detailedRow struct {
	id        uint64
	userID    uint32
	name      string
	email     string
	score     float64
	createdAt uint32
	blob      []byte
	meta      []api.ColMeta
	lens      []api.Ulint
}

func TestDetailedQueryHarness(t *testing.T) {
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
	if err := api.DatabaseCreate(customQueryDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}
	if err := createCustomQueryTable(); err != api.DB_SUCCESS {
		t.Fatalf("create table: %v", err)
	}
	rows := []customRow{
		{1, 10, "Alice", "alice@gmail.com", 82.5, 1700000001, "blob-a"},
		{2, 20, "Bob", "bob@yahoo.com", 55.0, 1700000002, "blob-b"},
		{3, 10, "Charlie", "charlie@gmail.com", 99.1, 1700000003, "blob-c"},
	}
	tableName := customQueryDB + "/" + customQueryTable
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, nil, &crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorOpenTable: %v", err)
	}
	if err := insertCustomQueryRows(crsr, rows); err != api.DB_SUCCESS {
		t.Fatalf("insert rows: %v", err)
	}
	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorClose: %v", err)
	}

	got, err := queryDetailedData(2, 1, 0)
	if err != api.DB_SUCCESS {
		t.Fatalf("queryDetailedData: %v", err)
	}
	if len(got) != 2 || got[0].id != 2 || got[1].id != 3 {
		t.Fatalf("unexpected ids: %+v", got)
	}

	got, err = queryDetailedData(1, 0, 3)
	if err != api.DB_SUCCESS {
		t.Fatalf("queryDetailedData specific: %v", err)
	}
	if len(got) != 1 || got[0].id != 3 {
		t.Fatalf("unexpected specific id results: %+v", got)
	}
	assertDetailedMeta(t, got[0], rows[2])

	if err := api.TableDrop(nil, tableName); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(customQueryDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func queryDetailedData(limit, offset uint, specificID uint64) ([]detailedRow, api.ErrCode) {
	if limit == 0 {
		limit = detailedDefaultLimit
	}
	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	tableName := customQueryDB + "/" + customQueryTable
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, trx, &crsr); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return nil, err
	}
	defer func() {
		_ = api.CursorClose(crsr)
	}()
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		_ = api.TrxRollback(trx)
		return nil, api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	err := api.CursorFirst(crsr)
	if err == api.DB_END_OF_INDEX {
		_ = api.TrxCommit(trx)
		return nil, api.DB_SUCCESS
	}
	if err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return nil, err
	}

	var skipped uint
	var out []detailedRow
	for err == api.DB_SUCCESS {
		err = api.CursorReadRow(crsr, tpl)
		if err != api.DB_SUCCESS {
			break
		}
		if specificID > 0 {
			var id uint64
			if err := api.TupleReadU64(tpl, 0, &id); err != api.DB_SUCCESS {
				break
			}
			if id != specificID {
				err = api.CursorNext(crsr)
				continue
			}
		}
		if skipped < offset {
			skipped++
		} else {
			row, snapErr := snapshotDetailedRow(tpl)
			if snapErr != api.DB_SUCCESS {
				err = snapErr
				break
			}
			out = append(out, row)
			if uint(len(out)) >= limit {
				break
			}
		}
		err = api.CursorNext(crsr)
		tpl = api.TupleClear(tpl)
	}

	if err == api.DB_END_OF_INDEX {
		err = api.DB_SUCCESS
	}
	if err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return out, err
	}
	if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
		return out, err
	}
	return out, api.DB_SUCCESS
}

func snapshotDetailedRow(tpl *data.Tuple) (detailedRow, api.ErrCode) {
	row := detailedRow{
		meta: make([]api.ColMeta, 7),
		lens: make([]api.Ulint, 7),
	}
	for i := range row.meta {
		row.lens[i] = api.ColGetMeta(tpl, i, &row.meta[i])
	}
	if err := api.TupleReadU64(tpl, 0, &row.id); err != api.DB_SUCCESS {
		return row, err
	}
	if err := api.TupleReadU32(tpl, 1, &row.userID); err != api.DB_SUCCESS {
		return row, err
	}
	row.name = string(api.ColGetValue(tpl, 2))
	row.email = string(api.ColGetValue(tpl, 3))
	if err := api.TupleReadDouble(tpl, 4, &row.score); err != api.DB_SUCCESS {
		return row, err
	}
	if err := api.TupleReadU32(tpl, 5, &row.createdAt); err != api.DB_SUCCESS {
		return row, err
	}
	row.blob = append([]byte(nil), api.ColGetValue(tpl, 6)...)
	return row, api.DB_SUCCESS
}

func assertDetailedMeta(t *testing.T, got detailedRow, want customRow) {
	t.Helper()
	expectMeta := []api.ColMeta{
		{Type: api.IB_INT, Attr: api.IB_COL_UNSIGNED, TypeLen: 8},
		{Type: api.IB_INT, Attr: api.IB_COL_UNSIGNED, TypeLen: 4},
		{Type: api.IB_VARCHAR, Attr: api.IB_COL_NONE, TypeLen: 100},
		{Type: api.IB_VARCHAR, Attr: api.IB_COL_NONE, TypeLen: 255},
		{Type: api.IB_DOUBLE, Attr: api.IB_COL_NONE, TypeLen: 8},
		{Type: api.IB_INT, Attr: api.IB_COL_UNSIGNED, TypeLen: 4},
		{Type: api.IB_BLOB, Attr: api.IB_COL_NONE, TypeLen: 0},
	}
	if len(got.meta) != len(expectMeta) {
		t.Fatalf("meta len=%d want=%d", len(got.meta), len(expectMeta))
	}
	for i := range expectMeta {
		if got.meta[i] != expectMeta[i] {
			t.Fatalf("meta[%d]=%+v want=%+v", i, got.meta[i], expectMeta[i])
		}
	}
	if got.name != want.name || got.email != want.email || got.userID != want.userID {
		t.Fatalf("row values mismatch: got=%+v want=%+v", got, want)
	}
	if got.score != want.score || got.createdAt != want.createdAt || string(got.blob) != want.blob {
		t.Fatalf("row payload mismatch: got=%+v want=%+v", got, want)
	}
	if got.lens[0] != 8 || got.lens[1] != 4 || got.lens[4] != 8 || got.lens[5] != 4 {
		t.Fatalf("unexpected numeric lengths: %+v", got.lens)
	}
	if got.lens[2] != api.Ulint(len(want.name)) || got.lens[3] != api.Ulint(len(want.email)) {
		t.Fatalf("unexpected string lengths: %+v", got.lens)
	}
	if got.lens[6] != api.Ulint(len(want.blob)) {
		t.Fatalf("unexpected blob length: %+v", got.lens)
	}
}
