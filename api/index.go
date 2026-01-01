package api

import (
	"strings"
	"sync/atomic"

	"github.com/wilhasse/innodb-go/btr"
	"github.com/wilhasse/innodb-go/trx"
)

var nextIndexID uint64

// IndexSchemaCreate creates an index schema for an existing table.
func IndexSchemaCreate(_ *trx.Trx, name string, tableName string, out **IndexSchema) ErrCode {
	if out == nil {
		return DB_ERROR
	}
	schemaMu.Lock()
	defer schemaMu.Unlock()
	table := findTableLocked(tableName)
	if table == nil || table.Schema == nil {
		return DB_TABLE_NOT_FOUND
	}
	index := &IndexSchema{Name: name, Table: table.Schema}
	table.Schema.Indexes = append(table.Schema.Indexes, index)
	*out = index
	return DB_SUCCESS
}

// IndexSchemaDelete releases an index schema.
func IndexSchemaDelete(_ *IndexSchema) {
}

// IndexCreate registers an index schema.
func IndexCreate(index *IndexSchema, indexID *uint64) ErrCode {
	if index == nil {
		return DB_ERROR
	}
	if indexID != nil {
		*indexID = atomic.AddUint64(&nextIndexID, 1)
	}
	return DB_SUCCESS
}

// CursorOpenIndexUsingName opens an index cursor by name.
func CursorOpenIndexUsingName(crsr *Cursor, indexName string, out **Cursor) ErrCode {
	if crsr == nil || crsr.Table == nil || crsr.Table.Schema == nil || out == nil {
		return DB_ERROR
	}
	for _, idx := range crsr.Table.Schema.Indexes {
		if idx != nil && strings.EqualFold(idx.Name, indexName) {
			var tree *btr.Tree
			if crsr.Table.Store != nil {
				tree = crsr.Table.Store.Tree
			}
			cursor := &Cursor{
				Table:     crsr.Table,
				Tree:      tree,
				Trx:       crsr.Trx,
				MatchMode: crsr.MatchMode,
				LockMode:  crsr.LockMode,
			}
			if tree != nil {
				cursor.pcur = btr.NewPcur(tree)
			}
			*out = cursor
			return DB_SUCCESS
		}
	}
	return DB_NOT_FOUND
}

func findTableLocked(name string) *Table {
	for _, db := range databases {
		if table := db.Tables[strings.ToLower(name)]; table != nil {
			return table
		}
	}
	return nil
}
