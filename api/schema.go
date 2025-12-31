package api

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/wilhasse/innodb-go/row"
	"github.com/wilhasse/innodb-go/trx"
)

// TableFormat mirrors ib_tbl_fmt_t.
type TableFormat int

const (
	IB_TBL_COMPACT TableFormat = iota
	IB_TBL_COMPRESSED
)

// TableSchema stores column and index metadata.
type TableSchema struct {
	Name     string
	Format   TableFormat
	PageSize int
	Columns  []ColumnSchema
	Indexes  []*IndexSchema
}

// ColumnSchema describes a column.
type ColumnSchema struct {
	Name  string
	Type  ColType
	Attr  ColAttr
	Size  uint32
	Flags uint32
}

// IndexSchema describes an index.
type IndexSchema struct {
	Name      string
	Columns   []string
	Prefixes  []int
	Clustered bool
	Table     *TableSchema
}

// Table holds schema and storage.
type Table struct {
	ID     uint64
	Schema *TableSchema
	Store  *row.Store
}

// Database holds tables.
type Database struct {
	Name   string
	Tables map[string]*Table
}

var (
	schemaMu   sync.Mutex
	databases  = map[string]*Database{}
	nextTableID uint64
)

// TableSchemaCreate initializes a table schema.
func TableSchemaCreate(name string, out **TableSchema, format TableFormat, pageSize int) ErrCode {
	if out == nil {
		return DB_ERROR
	}
	if format == IB_TBL_COMPRESSED && !validCompressedPageSize(pageSize) {
		return DB_INVALID_INPUT
	}
	*out = &TableSchema{Name: name, Format: format, PageSize: pageSize}
	return DB_SUCCESS
}

// TableSchemaAddCol appends a column to the schema.
func TableSchemaAddCol(schema *TableSchema, name string, typ ColType, attr ColAttr, flags uint32, size uint32) ErrCode {
	if schema == nil {
		return DB_ERROR
	}
	schema.Columns = append(schema.Columns, ColumnSchema{
		Name:  name,
		Type:  typ,
		Attr:  attr,
		Flags: flags,
		Size:  size,
	})
	return DB_SUCCESS
}

// TableSchemaAddIndex creates a new index schema.
func TableSchemaAddIndex(schema *TableSchema, name string, out **IndexSchema) ErrCode {
	if schema == nil {
		return DB_ERROR
	}
	index := &IndexSchema{Name: name, Table: schema}
	schema.Indexes = append(schema.Indexes, index)
	if out != nil {
		*out = index
	}
	return DB_SUCCESS
}

// IndexSchemaAddCol appends a column to an index schema.
func IndexSchemaAddCol(index *IndexSchema, name string, prefix int) ErrCode {
	if index == nil {
		return DB_ERROR
	}
	if prefix > 0 {
		col := findSchemaColumn(index.Table, name)
		if col == nil || !prefixAllowed(col.Type) {
			return DB_SCHEMA_ERROR
		}
	}
	index.Columns = append(index.Columns, name)
	index.Prefixes = append(index.Prefixes, prefix)
	return DB_SUCCESS
}

// IndexSchemaSetClustered marks an index as clustered.
func IndexSchemaSetClustered(index *IndexSchema) ErrCode {
	if index == nil {
		return DB_ERROR
	}
	index.Clustered = true
	return DB_SUCCESS
}

// TableSchemaDelete releases a schema.
func TableSchemaDelete(_ *TableSchema) {
}

// SchemaLockExclusive is a no-op lock stub.
func SchemaLockExclusive(trx *trx.Trx) ErrCode {
	if trx == nil {
		return DB_ERROR
	}
	lockSchema(trx)
	return DB_SUCCESS
}

// DatabaseCreate creates a database if needed.
func DatabaseCreate(name string) ErrCode {
	schemaMu.Lock()
	defer schemaMu.Unlock()
	key := strings.ToLower(name)
	if _, ok := databases[key]; ok {
		return DB_SUCCESS
	}
	databases[key] = &Database{Name: name, Tables: map[string]*Table{}}
	return DB_SUCCESS
}

// DatabaseDrop drops a database if it exists.
func DatabaseDrop(name string) ErrCode {
	schemaMu.Lock()
	defer schemaMu.Unlock()
	delete(databases, strings.ToLower(name))
	return DB_SUCCESS
}

// TableCreate registers a table schema and store.
func TableCreate(_ *trx.Trx, schema *TableSchema, tableID *uint64) ErrCode {
	if schema == nil {
		return DB_ERROR
	}
	dbName, tableName := splitTableName(schema.Name)
	if dbName == "" || tableName == "" {
		return DB_INVALID_INPUT
	}
	if err := DatabaseCreate(dbName); err != DB_SUCCESS {
		return err
	}

	schemaMu.Lock()
	defer schemaMu.Unlock()
	db := databases[strings.ToLower(dbName)]
	if db == nil {
		return DB_ERROR
	}
	if _, ok := db.Tables[strings.ToLower(schema.Name)]; ok {
		return DB_TABLE_IS_BEING_USED
	}
	id := atomic.AddUint64(&nextTableID, 1)
	if tableID != nil {
		*tableID = id
	}
	primaryKey := -1
	primaryKeyPrefix := 0
	if schema != nil {
		for _, idx := range schema.Indexes {
			if idx == nil || !idx.Clustered || len(idx.Columns) == 0 {
				continue
			}
			colName := strings.ToLower(idx.Columns[0])
			for i, col := range schema.Columns {
				if strings.ToLower(col.Name) == colName {
					primaryKey = i
					if len(idx.Prefixes) > 0 {
						primaryKeyPrefix = idx.Prefixes[0]
					}
					break
				}
			}
			if primaryKey >= 0 {
				break
			}
		}
	}
	store := row.NewStore(primaryKey)
	store.PrimaryKeyPrefix = primaryKeyPrefix
	db.Tables[strings.ToLower(schema.Name)] = &Table{ID: id, Schema: schema, Store: store}
	return DB_SUCCESS
}

// TableDrop removes a table.
func TableDrop(_ *trx.Trx, name string) ErrCode {
	dbName, _ := splitTableName(name)
	if dbName == "" {
		return DB_INVALID_INPUT
	}
	schemaMu.Lock()
	defer schemaMu.Unlock()
	db := databases[strings.ToLower(dbName)]
	if db == nil {
		return DB_TABLE_NOT_FOUND
	}
	delete(db.Tables, strings.ToLower(name))
	return DB_SUCCESS
}

func splitTableName(name string) (string, string) {
	parts := strings.Split(name, "/")
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

func validCompressedPageSize(size int) bool {
	switch size {
	case 0, 1, 2, 4, 8, 16:
		return true
	default:
		return false
	}
}

func findSchemaColumn(schema *TableSchema, name string) *ColumnSchema {
	if schema == nil {
		return nil
	}
	for i := range schema.Columns {
		if strings.EqualFold(schema.Columns[i].Name, name) {
			return &schema.Columns[i]
		}
	}
	return nil
}

func prefixAllowed(colType ColType) bool {
	switch colType {
	case IB_VARCHAR, IB_CHAR, IB_BINARY, IB_VARBINARY, IB_BLOB, IB_VARCHAR_ANYCHARSET, IB_CHAR_ANYCHARSET:
		return true
	default:
		return false
	}
}
