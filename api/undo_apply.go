package api

import (
	"fmt"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/rec"
	"github.com/wilhasse/innodb-go/trx"
)

func rollbackUndoRecords(ibTrx *trx.Trx) error {
	if ibTrx == nil || len(ibTrx.UndoRecords) == 0 {
		return nil
	}
	for i := len(ibTrx.UndoRecords) - 1; i >= 0; i-- {
		rec := ibTrx.UndoRecords[i]
		if err := applyUndoRecord(&rec); err != nil {
			return err
		}
	}
	return nil
}

func applyUndoRecord(rec *trx.UndoRecord) error {
	if rec == nil {
		return nil
	}
	table := findTableByID(rec.TableID)
	if table == nil || table.Store == nil {
		return fmt.Errorf("api: undo table %d not found", rec.TableID)
	}
	payload, err := trx.DecodeUndoPayload(rec.Data)
	if err != nil {
		return fmt.Errorf("api: undo payload decode: %w", err)
	}
	switch rec.Type {
	case trx.UndoInsertRec:
		if len(payload.PrimaryKey) == 0 {
			return fmt.Errorf("api: undo insert missing key")
		}
		row := table.Store.RowByKey(payload.PrimaryKey)
		if row == nil {
			return fmt.Errorf("api: undo insert row missing")
		}
		if !table.Store.RemoveTuple(row) {
			return fmt.Errorf("api: undo insert remove failed")
		}
	case trx.UndoUpdExistRec:
		if len(payload.BeforeImage) == 0 {
			return fmt.Errorf("api: undo update missing before image")
		}
		before, err := decodeUndoTuple(table, payload.BeforeImage)
		if err != nil {
			return err
		}
		if len(payload.PrimaryKey) == 0 && table.Store != nil {
			payload.PrimaryKey = table.Store.KeyForSearch(before, len(before.Fields))
		}
		row := table.Store.RowByKey(payload.PrimaryKey)
		if row == nil {
			return fmt.Errorf("api: undo update row missing")
		}
		if err := table.Store.ReplaceTuple(row, before); err != nil {
			return fmt.Errorf("api: undo update replace: %w", err)
		}
	case trx.UndoDelMarkRec:
		if len(payload.BeforeImage) == 0 {
			return fmt.Errorf("api: undo delete missing before image")
		}
		before, err := decodeUndoTuple(table, payload.BeforeImage)
		if err != nil {
			return err
		}
		if err := table.Store.Insert(before); err != nil {
			return fmt.Errorf("api: undo delete insert: %w", err)
		}
	}
	return nil
}

func decodeUndoTuple(table *Table, before []byte) (*data.Tuple, error) {
	if table == nil {
		return nil, fmt.Errorf("api: undo missing table")
	}
	nFields := 0
	if table.Schema != nil {
		nFields = len(table.Schema.Columns)
	}
	if nFields == 0 && table.Store != nil && len(table.Store.Rows) > 0 {
		if row := table.Store.Rows[0]; row != nil {
			nFields = len(row.Fields)
		}
	}
	if nFields == 0 {
		return nil, fmt.Errorf("api: undo missing field count")
	}
	decoded, err := rec.DecodeVar(before, nFields, 0)
	if err != nil {
		return nil, fmt.Errorf("api: undo decode before image: %w", err)
	}
	return decoded, nil
}

func findTableByID(id uint64) *Table {
	if id == 0 {
		return nil
	}
	schemaMu.Lock()
	defer schemaMu.Unlock()
	for _, db := range databases {
		for _, table := range db.Tables {
			if table != nil && table.ID == id {
				return table
			}
		}
	}
	return nil
}
