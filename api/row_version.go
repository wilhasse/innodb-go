package api

import "github.com/wilhasse/innodb-go/data"

func recordRowVersionForKey(crsr *Cursor, key []byte, tpl *data.Tuple) {
	if crsr == nil || crsr.Table == nil || crsr.Table.Store == nil {
		return
	}
	if len(key) == 0 && tpl != nil {
		key = primaryKeyBytes(crsr.Table.Store, tpl)
	}
	if len(key) == 0 {
		return
	}
	var trxID uint64
	if crsr.Trx != nil {
		trxID = crsr.Trx.ID
	}
	crsr.Table.Store.RecordVersion(key, trxID, tpl)
}
