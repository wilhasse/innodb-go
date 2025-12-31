package tests

import (
	"fmt"
	"strings"
	"testing"

	"github.com/wilhasse/innodb-go/api"
	"github.com/wilhasse/innodb-go/data"
)

const (
	customQueryDB    = "bulk_test"
	customQueryTable = "massive_data"
	customQueryLimit = 20
)

type customQueryParams struct {
	specificID       uint64
	specificUserID   uint32
	rangeStart       uint64
	rangeEnd         uint64
	limit            uint
	offset           uint
	scoreMin         float64
	scoreMax         float64
	nameLike         string
	emailDomain      string
	countOnly        bool
	useSpecificID    bool
	useSpecificUserID bool
	useRange         bool
	useScoreFilter   bool
	useNameFilter    bool
	useEmailFilter   bool
}

type queryResult struct {
	ids       []uint64
	processed uint
	matching  uint
	displayed uint
}

type customRow struct {
	id        uint64
	userID    uint32
	name      string
	email     string
	score     float64
	createdAt uint32
	blob      string
}

func TestCustomQueryHarness(t *testing.T) {
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

	tableName := fmt.Sprintf("%s/%s", customQueryDB, customQueryTable)
	var crsr *api.Cursor
	if err := api.CursorOpenTable(tableName, nil, &crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorOpenTable: %v", err)
	}
	rows := []customRow{
		{1, 10, "Alice", "alice@gmail.com", 82.5, 1700000001, "blob-a"},
		{2, 20, "Bob", "bob@yahoo.com", 55.0, 1700000002, "blob-b"},
		{3, 10, "Charlie", "charlie@gmail.com", 99.1, 1700000003, "blob-c"},
		{4, 30, "Dora", "dora@test.org", 45.5, 1700000004, "blob-d"},
		{5, 20, "Eve", "eve@gmail.com", 88.8, 1700000005, "blob-e"},
		{6, 40, "Frank", "frank@company.com", 77.7, 1700000006, "blob-f"},
	}
	if err := insertCustomQueryRows(crsr, rows); err != api.DB_SUCCESS {
		t.Fatalf("insert rows: %v", err)
	}
	if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
		t.Fatalf("CursorClose: %v", err)
	}

	cases := []struct {
		name   string
		params customQueryParams
		want   []uint64
	}{
		{
			name: "specific id",
			params: customQueryParams{
				specificID:    3,
				useSpecificID: true,
			},
			want: []uint64{3},
		},
		{
			name: "user id",
			params: customQueryParams{
				specificUserID:   20,
				useSpecificUserID: true,
			},
			want: []uint64{2, 5},
		},
		{
			name: "range",
			params: customQueryParams{
				rangeStart: 2,
				rangeEnd:   4,
				useRange:   true,
			},
			want: []uint64{2, 3, 4},
		},
		{
			name: "score filter",
			params: customQueryParams{
				scoreMin:       80.0,
				scoreMax:       90.0,
				useScoreFilter: true,
			},
			want: []uint64{1, 5},
		},
		{
			name: "name like",
			params: customQueryParams{
				nameLike:      "ar",
				useNameFilter: true,
			},
			want: []uint64{3},
		},
		{
			name: "email domain",
			params: customQueryParams{
				emailDomain:     "gmail.com",
				useEmailFilter:  true,
			},
			want: []uint64{1, 3, 5},
		},
		{
			name: "offset and limit",
			params: customQueryParams{
				rangeStart: 1,
				rangeEnd:   6,
				useRange:   true,
				offset:     2,
				limit:      2,
			},
			want: []uint64{3, 4},
		},
		{
			name: "count only",
			params: customQueryParams{
				emailDomain:     "gmail.com",
				useEmailFilter:  true,
				countOnly:       true,
			},
			want: nil,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			res, err := executeCustomQuery(tc.params)
			if err != api.DB_SUCCESS {
				t.Fatalf("executeCustomQuery: %v", err)
			}
			assertIDList(t, res.ids, tc.want)
		})
	}

	if err := api.TableDrop(nil, tableName); err != api.DB_SUCCESS {
		t.Fatalf("TableDrop: %v", err)
	}
	if err := api.DatabaseDrop(customQueryDB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseDrop: %v", err)
	}
}

func createCustomQueryTable() api.ErrCode {
	fullName := fmt.Sprintf("%s/%s", customQueryDB, customQueryTable)
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(fullName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "id", api.IB_INT, api.IB_COL_UNSIGNED, 0, 8); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "user_id", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "name", api.IB_VARCHAR, api.IB_COL_NONE, 0, 100); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "email", api.IB_VARCHAR, api.IB_COL_NONE, 0, 255); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "score", api.IB_DOUBLE, api.IB_COL_NONE, 0, 8); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "created_at", api.IB_INT, api.IB_COL_UNSIGNED, 0, 4); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "data_blob", api.IB_BLOB, api.IB_COL_NONE, 0, 0); err != api.DB_SUCCESS {
		return err
	}

	var idx *api.IndexSchema
	if err := api.TableSchemaAddIndex(schema, "PRIMARY_KEY", &idx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaAddCol(idx, "id", 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.IndexSchemaSetClustered(idx); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableCreate(nil, schema, nil); err != api.DB_SUCCESS {
		return err
	}
	api.TableSchemaDelete(schema)
	return api.DB_SUCCESS
}

func insertCustomQueryRows(crsr *api.Cursor, rows []customRow) api.ErrCode {
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return api.DB_ERROR
	}
	for _, row := range rows {
		if err := api.TupleWriteU64(tpl, 0, row.id); err != api.DB_SUCCESS {
			return err
		}
		if err := api.TupleWriteU32(tpl, 1, row.userID); err != api.DB_SUCCESS {
			return err
		}
		if err := api.ColSetValue(tpl, 2, []byte(row.name), len(row.name)); err != api.DB_SUCCESS {
			return err
		}
		if err := api.ColSetValue(tpl, 3, []byte(row.email), len(row.email)); err != api.DB_SUCCESS {
			return err
		}
		if err := api.TupleWriteDouble(tpl, 4, row.score); err != api.DB_SUCCESS {
			return err
		}
		if err := api.TupleWriteU32(tpl, 5, row.createdAt); err != api.DB_SUCCESS {
			return err
		}
		if err := api.ColSetValue(tpl, 6, []byte(row.blob), len(row.blob)); err != api.DB_SUCCESS {
			return err
		}
		if err := api.CursorInsertRow(crsr, tpl); err != api.DB_SUCCESS {
			return err
		}
		tpl = api.TupleClear(tpl)
	}
	api.TupleDelete(tpl)
	return api.DB_SUCCESS
}

func executeCustomQuery(params customQueryParams) (queryResult, api.ErrCode) {
	if params.limit == 0 {
		params.limit = customQueryLimit
	}

	trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
	var crsr *api.Cursor
	tableName := fmt.Sprintf("%s/%s", customQueryDB, customQueryTable)
	if err := api.CursorOpenTable(tableName, trx, &crsr); err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return queryResult{}, err
	}
	defer func() {
		_ = api.CursorClose(crsr)
	}()

	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		_ = api.TrxRollback(trx)
		return queryResult{}, api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	result := queryResult{}
	err := api.CursorFirst(crsr)
	if err == api.DB_END_OF_INDEX {
		_ = api.TrxCommit(trx)
		return result, api.DB_SUCCESS
	}
	if err != api.DB_SUCCESS {
		_ = api.TrxRollback(trx)
		return result, err
	}

	var skipped uint
	for err == api.DB_SUCCESS {
		err = api.CursorReadRow(crsr, tpl)
		if err != api.DB_SUCCESS {
			break
		}
		result.processed++
		match, id, matchErr := rowMatchesFilters(tpl, params)
		if matchErr != api.DB_SUCCESS {
			err = matchErr
			break
		}
		if match {
			result.matching++
			if skipped < params.offset {
				skipped++
			} else {
				if !params.countOnly {
					result.ids = append(result.ids, id)
				}
				result.displayed++
				if result.displayed >= params.limit {
					break
				}
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
		return result, err
	}
	if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
		return result, err
	}
	return result, api.DB_SUCCESS
}

func rowMatchesFilters(tpl *data.Tuple, params customQueryParams) (bool, uint64, api.ErrCode) {
	var id uint64
	if err := api.TupleReadU64(tpl, 0, &id); err != api.DB_SUCCESS {
		return false, 0, err
	}
	if params.useSpecificID && id != params.specificID {
		return false, id, api.DB_SUCCESS
	}
	if params.useRange && (id < params.rangeStart || id > params.rangeEnd) {
		return false, id, api.DB_SUCCESS
	}
	if params.useSpecificUserID {
		var userID uint32
		if err := api.TupleReadU32(tpl, 1, &userID); err != api.DB_SUCCESS {
			return false, id, err
		}
		if userID != params.specificUserID {
			return false, id, api.DB_SUCCESS
		}
	}
	if params.useScoreFilter {
		var score float64
		if err := api.TupleReadDouble(tpl, 4, &score); err != api.DB_SUCCESS {
			return false, id, err
		}
		if score < params.scoreMin || score > params.scoreMax {
			return false, id, api.DB_SUCCESS
		}
	}
	if params.useNameFilter {
		nameLen := api.ColGetLen(tpl, 2)
		nameVal := api.ColGetValue(tpl, 2)
		if nameLen == api.Ulint(api.IBSQLNull) || len(nameVal) == 0 {
			return false, id, api.DB_SUCCESS
		}
		if !strings.Contains(string(nameVal), params.nameLike) {
			return false, id, api.DB_SUCCESS
		}
	}
	if params.useEmailFilter {
		emailLen := api.ColGetLen(tpl, 3)
		emailVal := api.ColGetValue(tpl, 3)
		if emailLen == api.Ulint(api.IBSQLNull) || len(emailVal) == 0 {
			return false, id, api.DB_SUCCESS
		}
		if !strings.Contains(string(emailVal), params.emailDomain) {
			return false, id, api.DB_SUCCESS
		}
	}
	return true, id, api.DB_SUCCESS
}

func assertIDList(t *testing.T, got, want []uint64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("ids len=%d want=%d got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ids[%d]=%d want=%d got=%v", i, got[i], want[i], got)
		}
	}
}
