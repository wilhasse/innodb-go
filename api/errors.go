package api

// ErrCode mirrors InnoDB's db_err values.
type ErrCode int

const (
	DB_SUCCESS                    ErrCode = 10
	DB_ERROR                      ErrCode = 11
	DB_INTERRUPTED                ErrCode = 12
	DB_OUT_OF_MEMORY              ErrCode = 13
	DB_OUT_OF_FILE_SPACE          ErrCode = 14
	DB_LOCK_WAIT                  ErrCode = 15
	DB_DEADLOCK                   ErrCode = 16
	DB_ROLLBACK                   ErrCode = 17
	DB_DUPLICATE_KEY              ErrCode = 18
	DB_QUE_THR_SUSPENDED          ErrCode = 19
	DB_MISSING_HISTORY            ErrCode = 20
	DB_CLUSTER_NOT_FOUND          ErrCode = 30
	DB_TABLE_NOT_FOUND            ErrCode = 31
	DB_MUST_GET_MORE_FILE_SPACE   ErrCode = 32
	DB_TABLE_IS_BEING_USED        ErrCode = 33
	DB_TOO_BIG_RECORD             ErrCode = 34
	DB_LOCK_WAIT_TIMEOUT          ErrCode = 35
	DB_NO_REFERENCED_ROW          ErrCode = 36
	DB_ROW_IS_REFERENCED          ErrCode = 37
	DB_CANNOT_ADD_CONSTRAINT      ErrCode = 38
	DB_CORRUPTION                 ErrCode = 39
	DB_COL_APPEARS_TWICE_IN_INDEX ErrCode = 40
	DB_CANNOT_DROP_CONSTRAINT     ErrCode = 41
	DB_NO_SAVEPOINT               ErrCode = 42
	DB_TABLESPACE_ALREADY_EXISTS  ErrCode = 43
	DB_TABLESPACE_DELETED         ErrCode = 44
	DB_LOCK_TABLE_FULL            ErrCode = 45
	DB_FOREIGN_DUPLICATE_KEY      ErrCode = 46
	DB_TOO_MANY_CONCURRENT_TRXS   ErrCode = 47
	DB_UNSUPPORTED                ErrCode = 48
	DB_PRIMARY_KEY_IS_NULL        ErrCode = 49
	DB_FATAL                      ErrCode = 50
	DB_FAIL                       ErrCode = 1000
	DB_OVERFLOW                   ErrCode = 1001
	DB_UNDERFLOW                  ErrCode = 1002
	DB_STRONG_FAIL                ErrCode = 1003
	DB_ZIP_OVERFLOW               ErrCode = 1004
	DB_RECORD_NOT_FOUND           ErrCode = 1500
	DB_END_OF_INDEX               ErrCode = 1501
	DB_SCHEMA_ERROR               ErrCode = 2000
	DB_DATA_MISMATCH              ErrCode = 2001
	DB_SCHEMA_NOT_LOCKED          ErrCode = 2002
	DB_NOT_FOUND                  ErrCode = 2003
	DB_READONLY                   ErrCode = 2004
	DB_INVALID_INPUT              ErrCode = 2005
)

func (code ErrCode) Error() string {
	return ErrString(code)
}

// ErrString returns a human-readable message for an error code.
func ErrString(code ErrCode) string {
	switch code {
	case DB_SUCCESS:
		return "Success"
	case DB_ERROR:
		return "Generic error"
	case DB_OUT_OF_MEMORY:
		return "Cannot allocate memory"
	case DB_OUT_OF_FILE_SPACE:
		return "Out of disk space"
	case DB_LOCK_WAIT:
		return "Lock wait"
	case DB_DEADLOCK:
		return "Deadlock"
	case DB_ROLLBACK:
		return "Rollback"
	case DB_DUPLICATE_KEY:
		return "Duplicate key"
	case DB_QUE_THR_SUSPENDED:
		return "The queue thread has been suspended"
	case DB_MISSING_HISTORY:
		return "Required history data has been deleted"
	case DB_CLUSTER_NOT_FOUND:
		return "Cluster not found"
	case DB_TABLE_NOT_FOUND:
		return "Table not found"
	case DB_MUST_GET_MORE_FILE_SPACE:
		return "More file space needed"
	case DB_TABLE_IS_BEING_USED:
		return "Table is being used"
	case DB_TOO_BIG_RECORD:
		return "Record too big"
	case DB_LOCK_WAIT_TIMEOUT:
		return "Lock wait timeout"
	case DB_NO_REFERENCED_ROW:
		return "Referenced key value not found"
	case DB_ROW_IS_REFERENCED:
		return "Row is referenced"
	case DB_CANNOT_ADD_CONSTRAINT:
		return "Cannot add constraint"
	case DB_CORRUPTION:
		return "Data structure corruption"
	case DB_COL_APPEARS_TWICE_IN_INDEX:
		return "Column appears twice in index"
	case DB_CANNOT_DROP_CONSTRAINT:
		return "Cannot drop constraint"
	case DB_NO_SAVEPOINT:
		return "No such savepoint"
	case DB_TABLESPACE_ALREADY_EXISTS:
		return "Tablespace already exists"
	case DB_TABLESPACE_DELETED:
		return "No such tablespace"
	case DB_LOCK_TABLE_FULL:
		return "Lock structs have exhausted the buffer pool"
	case DB_FOREIGN_DUPLICATE_KEY:
		return "Foreign key activated with duplicate keys"
	case DB_TOO_MANY_CONCURRENT_TRXS:
		return "Too many concurrent transactions"
	case DB_UNSUPPORTED:
		return "Unsupported"
	case DB_PRIMARY_KEY_IS_NULL:
		return "Primary key is NULL"
	case DB_FAIL:
		return "Failed, retry may succeed"
	case DB_OVERFLOW:
		return "Overflow"
	case DB_UNDERFLOW:
		return "Underflow"
	case DB_STRONG_FAIL:
		return "Failed, retry will not succeed"
	case DB_ZIP_OVERFLOW:
		return "Zip overflow"
	case DB_RECORD_NOT_FOUND:
		return "Record not found"
	case DB_END_OF_INDEX:
		return "End of index"
	case DB_SCHEMA_ERROR:
		return "Error while validating a table or index schema"
	case DB_DATA_MISMATCH:
		return "Type mismatch"
	case DB_SCHEMA_NOT_LOCKED:
		return "Schema not locked"
	case DB_NOT_FOUND:
		return "Not found"
	case DB_READONLY:
		return "Readonly"
	case DB_INVALID_INPUT:
		return "Invalid input"
	case DB_FATAL:
		return "InnoDB fatal error"
	case DB_INTERRUPTED:
		return "Operation interrupted"
	default:
		return "Unknown error"
	}
}

// Err returns nil for DB_SUCCESS and the ErrCode otherwise.
func Err(code ErrCode) error {
	if code == DB_SUCCESS {
		return nil
	}
	return code
}
