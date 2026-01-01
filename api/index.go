package api

import (
	"errors"
	"strings"
	"sync/atomic"

	"github.com/wilhasse/innodb-go/btr"
	"github.com/wilhasse/innodb-go/row"
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
	if index.Clustered {
		return DB_SUCCESS
	}
	schemaMu.Lock()
	defer schemaMu.Unlock()
	table := findTableBySchemaLocked(index.Table)
	if table == nil || table.Store == nil {
		return DB_TABLE_NOT_FOUND
	}
	fields, err := indexColumnPositions(index.Table, index)
	if err != nil {
		return DB_SCHEMA_ERROR
	}
	if err := table.Store.AddSecondaryIndex(index.Name, fields, index.Prefixes, index.Unique); err != nil {
		if errors.Is(err, row.ErrDuplicateKey) {
			return DB_DUPLICATE_KEY
		}
		return DB_ERROR
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
			var sec *row.SecondaryIndex
			if crsr.Table.Store != nil {
				if idx.Clustered {
					tree = crsr.Table.Store.Tree
				} else {
					sec = crsr.Table.Store.SecondaryIndex(idx.Name)
					if sec != nil {
						tree = sec.Tree
					}
				}
			}
			if tree == nil {
				return DB_NOT_FOUND
			}
			cursor := &Cursor{
				Table:     crsr.Table,
				Tree:      tree,
				Index:     sec,
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

func findTableBySchemaLocked(schema *TableSchema) *Table {
	if schema == nil {
		return nil
	}
	for _, db := range databases {
		for _, table := range db.Tables {
			if table != nil && table.Schema == schema {
				return table
			}
		}
	}
	return nil
}

func indexColumnPositions(schema *TableSchema, index *IndexSchema) ([]int, error) {
	if schema == nil || index == nil {
		return nil, errors.New("index: missing schema")
	}
	positions := make([]int, 0, len(index.Columns))
	for _, name := range index.Columns {
		pos := -1
		for i, col := range schema.Columns {
			if strings.EqualFold(col.Name, name) {
				pos = i
				break
			}
		}
		if pos < 0 {
			return nil, errors.New("index: column not found")
		}
		positions = append(positions, pos)
	}
	if len(positions) == 0 {
		return nil, errors.New("index: empty column list")
	}
	return positions, nil
}
