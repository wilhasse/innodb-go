package api

import (
	"hash/fnv"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/lock"
	"github.com/wilhasse/innodb-go/rec"
)

func lockTableForDML(crsr *Cursor) ErrCode {
	if crsr == nil || crsr.Table == nil {
		return DB_ERROR
	}
	if crsr.Trx == nil {
		return DB_SUCCESS
	}
	tableName := tableLockName(crsr.Table)
	_, status := lock.LockTable(crsr.Trx, tableName, lock.ModeIX)
	return lockStatusToErr(status)
}

func lockRecordForDML(crsr *Cursor, tpl *data.Tuple, mode lock.Mode) ErrCode {
	if crsr == nil || crsr.Table == nil {
		return DB_ERROR
	}
	if crsr.Trx == nil {
		return DB_SUCCESS
	}
	key := lockRecordKey(crsr.Table, tpl)
	_, status := lock.LockRec(crsr.Trx, key, mode)
	return lockStatusToErr(status)
}

func lockStatusToErr(status lock.Status) ErrCode {
	switch status {
	case lock.LockGranted:
		return DB_SUCCESS
	case lock.LockWait:
		return DB_LOCK_WAIT
	case lock.LockDeadlock:
		return DB_DEADLOCK
	case lock.LockWaitTimeout:
		return DB_LOCK_WAIT_TIMEOUT
	default:
		return DB_ERROR
	}
}

func tableLockName(table *Table) string {
	if table == nil || table.Schema == nil {
		return ""
	}
	return table.Schema.Name
}

func lockRecordKey(table *Table, tpl *data.Tuple) lock.RecordKey {
	name := tableLockName(table)
	var key []byte
	if table != nil && table.Store != nil {
		key = primaryKeyBytes(table.Store, tpl)
	}
	if len(key) == 0 && tpl != nil {
		if encoded, err := rec.EncodeVar(tpl, nil, 0); err == nil {
			key = encoded
		}
	}
	hasher := fnv.New32a()
	if len(key) > 0 {
		_, _ = hasher.Write(key)
	} else if name != "" {
		_, _ = hasher.Write([]byte(name))
	}
	sum := hasher.Sum32()
	return lock.RecordKey{Table: name, PageNo: 0, HeapNo: uint16(sum)}
}
