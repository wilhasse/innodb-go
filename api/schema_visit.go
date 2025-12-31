package api

import (
	"strings"

	"github.com/wilhasse/innodb-go/trx"
)

// SchemaVisitorMode mirrors ib_schema_visitor_mode_t.
type SchemaVisitorMode int

const (
	SchemaVisitorTableAndIndexCol SchemaVisitorMode = iota
)

// SchemaVisitor mirrors ib_schema_visitor_t.
type SchemaVisitor struct {
	Mode        SchemaVisitorMode
	VisitTable  func(arg any, name string, tblFmt TableFormat, pageSize Ulint, nCols int, nIndexes int) int
	VisitColumn func(arg any, name string, colType ColType, length Ulint, attr ColAttr) int
	VisitIndex  func(arg any, name string, clustered Bool, unique Bool, nCols int) int
	VisitIndexColumn func(arg any, name string, prefixLen Ulint) int
}

// SchemaTableIterFunc iterates over table names.
type SchemaTableIterFunc func(arg any, name string, length int) int

var systemTables = map[string]*TableSchema{
	"SYS_TABLES":  {Name: "SYS_TABLES"},
	"SYS_COLUMNS": {Name: "SYS_COLUMNS"},
	"SYS_INDEXES": {Name: "SYS_INDEXES"},
}

// TableSchemaVisit visits a table schema with callbacks.
func TableSchemaVisit(trx *trx.Trx, name string, visitor *SchemaVisitor, arg any) ErrCode {
	if !isSchemaLocked(trx) {
		return DB_SCHEMA_NOT_LOCKED
	}
	table := findTable(name)
	var schema *TableSchema
	if table != nil {
		schema = table.Schema
	} else {
		schema = systemTables[strings.ToUpper(name)]
	}
	if schema == nil {
		return DB_TABLE_NOT_FOUND
	}
	if visitor == nil {
		return DB_SUCCESS
	}
	if visitor.VisitTable != nil {
		if visitor.VisitTable(arg, name, schema.Format, Ulint(schema.PageSize), len(schema.Columns), len(schema.Indexes)) != 0 {
			return DB_ERROR
		}
	}
	if visitor.VisitColumn != nil {
		for _, col := range schema.Columns {
			if visitor.VisitColumn(arg, col.Name, col.Type, Ulint(col.Size), col.Attr) != 0 {
				return DB_ERROR
			}
		}
	}
	if visitor.VisitIndex != nil || visitor.VisitIndexColumn != nil {
		for _, idx := range schema.Indexes {
			if idx == nil {
				continue
			}
			if visitor.VisitIndex != nil {
				clustered := IBFalse
				if idx.Clustered {
					clustered = IBTrue
				}
				unique := IBFalse
				if idx.Unique {
					unique = IBTrue
				}
				if visitor.VisitIndex(arg, idx.Name, clustered, unique, len(idx.Columns)) != 0 {
					return DB_ERROR
				}
			}
			if visitor.VisitIndexColumn != nil {
				for _, col := range idx.Columns {
					if visitor.VisitIndexColumn(arg, col, 0) != 0 {
						return DB_ERROR
					}
				}
			}
		}
	}
	return DB_SUCCESS
}

// SchemaTablesIterate iterates over all tables.
func SchemaTablesIterate(trx *trx.Trx, fn SchemaTableIterFunc, arg any) ErrCode {
	if !isSchemaLocked(trx) {
		return DB_SCHEMA_NOT_LOCKED
	}
	if fn == nil {
		return DB_SUCCESS
	}
	schemaMu.Lock()
	defer schemaMu.Unlock()
	for _, db := range databases {
		for name := range db.Tables {
			if fn(arg, name, len(name)) != 0 {
				return DB_ERROR
			}
		}
	}
	return DB_SUCCESS
}
