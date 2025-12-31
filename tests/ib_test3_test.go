package tests

import (
	"fmt"
	"testing"

	"github.com/wilhasse/innodb-go/api"
)

const (
	test3DB    = "test3_db"
	test3Table = "t"
)

func TestTest3Harness(t *testing.T) {
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
	if err := api.DatabaseCreate(test3DB); err != api.DB_SUCCESS {
		t.Fatalf("DatabaseCreate: %v", err)
	}

	for _, size := range []int{8, 16, 32, 64} {
		if err := createTest3Table(test3TableName(size), size); err != api.DB_SUCCESS {
			t.Fatalf("create table %d: %v", size, err)
		}
		trx := api.TrxBegin(api.IB_TRX_REPEATABLE_READ)
		if trx == nil {
			t.Fatalf("TrxBegin returned nil")
		}
		var crsr *api.Cursor
		if err := api.CursorOpenTable(test3TableName(size), trx, &crsr); err != api.DB_SUCCESS {
			t.Fatalf("CursorOpenTable: %v", err)
		}
		if err := api.CursorLock(crsr, api.LockIX); err != api.DB_SUCCESS {
			t.Fatalf("CursorLock: %v", err)
		}
		if err := insertTest3Rows(crsr, size); err != api.DB_SUCCESS {
			t.Fatalf("insert rows %d: %v", size, err)
		}
		if err := readTest3Rows(crsr, size); err != api.DB_SUCCESS {
			t.Fatalf("read rows %d: %v", size, err)
		}
		if err := api.CursorClose(crsr); err != api.DB_SUCCESS {
			t.Fatalf("CursorClose: %v", err)
		}
		if err := api.TrxCommit(trx); err != api.DB_SUCCESS {
			t.Fatalf("TrxCommit: %v", err)
		}
		if err := api.TableDrop(nil, test3TableName(size)); err != api.DB_SUCCESS {
			t.Fatalf("TableDrop: %v", err)
		}
	}
}

func createTest3Table(tableName string, size int) api.ErrCode {
	var schema *api.TableSchema
	if err := api.TableSchemaCreate(tableName, &schema, api.IB_TBL_COMPACT, 0); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c1", api.IB_INT, api.IB_COL_NONE, 0, uint32(size/8)); err != api.DB_SUCCESS {
		return err
	}
	if err := api.TableSchemaAddCol(schema, "c2", api.IB_INT, api.IB_COL_UNSIGNED, 0, uint32(size/8)); err != api.DB_SUCCESS {
		return err
	}
	err := api.TableCreate(nil, schema, nil)
	api.TableSchemaDelete(schema)
	return err
}

func insertTest3Rows(crsr *api.Cursor, size int) api.ErrCode {
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	for i := 0; i < 100; i++ {
		u := uint64(i + 1)
		switch size {
		case 8:
			if err := api.TupleWriteI8(tpl, 0, int8(-int64(u))); err != api.DB_SUCCESS {
				return err
			}
			if err := api.TupleWriteU8(tpl, 1, uint8(u)); err != api.DB_SUCCESS {
				return err
			}
		case 16:
			if err := api.TupleWriteI16(tpl, 0, int16(-int64(u))); err != api.DB_SUCCESS {
				return err
			}
			if err := api.TupleWriteU16(tpl, 1, uint16(u)); err != api.DB_SUCCESS {
				return err
			}
		case 32:
			if err := api.TupleWriteI32(tpl, 0, int32(-int64(u))); err != api.DB_SUCCESS {
				return err
			}
			if err := api.TupleWriteU32(tpl, 1, uint32(u)); err != api.DB_SUCCESS {
				return err
			}
		case 64:
			if err := api.TupleWriteI64(tpl, 0, int64(-int64(u))); err != api.DB_SUCCESS {
				return err
			}
			if err := api.TupleWriteU64(tpl, 1, u); err != api.DB_SUCCESS {
				return err
			}
		default:
			return api.DB_ERROR
		}
		if err := api.CursorInsertRow(crsr, tpl); err != api.DB_SUCCESS {
			return err
		}
		tpl = api.TupleClear(tpl)
	}
	return api.DB_SUCCESS
}

func readTest3Rows(crsr *api.Cursor, size int) api.ErrCode {
	if err := api.CursorFirst(crsr); err != api.DB_SUCCESS {
		if err == api.DB_RECORD_NOT_FOUND {
			return api.DB_SUCCESS
		}
		return err
	}
	tpl := api.ClustReadTupleCreate(crsr)
	if tpl == nil {
		return api.DB_ERROR
	}
	defer api.TupleDelete(tpl)

	var signedSum int64
	var unsignedSum int64
	for {
		err := api.CursorReadRow(crsr, tpl)
		if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
			break
		}
		if err != api.DB_SUCCESS {
			return err
		}
		switch size {
		case 8:
			var s int8
			var u uint8
			if err := api.TupleReadI8(tpl, 0, &s); err != api.DB_SUCCESS {
				return err
			}
			if err := api.TupleReadU8(tpl, 1, &u); err != api.DB_SUCCESS {
				return err
			}
			signedSum += int64(-s)
			unsignedSum += int64(u)
		case 16:
			var s int16
			var u uint16
			if err := api.TupleReadI16(tpl, 0, &s); err != api.DB_SUCCESS {
				return err
			}
			if err := api.TupleReadU16(tpl, 1, &u); err != api.DB_SUCCESS {
				return err
			}
			signedSum += int64(-s)
			unsignedSum += int64(u)
		case 32:
			var s int32
			var u uint32
			if err := api.TupleReadI32(tpl, 0, &s); err != api.DB_SUCCESS {
				return err
			}
			if err := api.TupleReadU32(tpl, 1, &u); err != api.DB_SUCCESS {
				return err
			}
			signedSum += int64(-s)
			unsignedSum += int64(u)
		case 64:
			var s int64
			var u uint64
			if err := api.TupleReadI64(tpl, 0, &s); err != api.DB_SUCCESS {
				return err
			}
			if err := api.TupleReadU64(tpl, 1, &u); err != api.DB_SUCCESS {
				return err
			}
			signedSum += -s
			unsignedSum += int64(u)
		default:
			return api.DB_ERROR
		}
		if err := api.CursorNext(crsr); err != api.DB_SUCCESS {
			if err == api.DB_END_OF_INDEX || err == api.DB_RECORD_NOT_FOUND {
				break
			}
			return err
		}
		tpl = api.TupleClear(tpl)
	}
	if signedSum != unsignedSum {
		return api.DB_ERROR
	}
	return api.DB_SUCCESS
}

func test3TableName(size int) string {
	return test3DB + "/" + test3Table + fmt.Sprintf("%d", size)
}
