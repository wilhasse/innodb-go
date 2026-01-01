package api

import (
	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/rec"
	"github.com/wilhasse/innodb-go/row"
	"github.com/wilhasse/innodb-go/trx"
)

func recordUndoInsert(crsr *Cursor, rowTpl *data.Tuple) {
	recordUndo(crsr, trx.UndoInsertRec, rowTpl, nil)
}

func recordUndoUpdate(crsr *Cursor, rowTpl *data.Tuple, before []byte) {
	recordUndo(crsr, trx.UndoUpdExistRec, rowTpl, before)
}

func recordUndoDelete(crsr *Cursor, rowTpl *data.Tuple, before []byte) {
	recordUndo(crsr, trx.UndoDelMarkRec, rowTpl, before)
}

func recordUndo(crsr *Cursor, recType uint8, rowTpl *data.Tuple, before []byte) {
	if crsr == nil || crsr.Trx == nil || crsr.Table == nil || crsr.Table.Store == nil || rowTpl == nil {
		return
	}
	pk := primaryKeyBytes(crsr.Table.Store, rowTpl)
	payload := trx.UndoPayload{
		TrxID:       crsr.Trx.ID,
		PrimaryKey:  pk,
		BeforeImage: before,
	}
	rec := trx.UndoRecord{
		Type:    recType,
		TableID: crsr.Table.ID,
		Data:    trx.EncodeUndoPayload(&payload),
	}
	trx.AppendUndoRecord(crsr.Trx, rec)
}

func encodeUndoImage(tpl *data.Tuple) []byte {
	if tpl == nil {
		return nil
	}
	bytes, err := rec.EncodeVar(tpl, nil, 0)
	if err != nil {
		return nil
	}
	return bytes
}

func primaryKeyBytes(store *row.Store, rowTpl *data.Tuple) []byte {
	if store == nil || rowTpl == nil {
		return nil
	}
	if key := store.KeyForRow(rowTpl); len(key) > 0 {
		return key
	}
	return store.KeyForSearch(rowTpl, len(rowTpl.Fields))
}
