package api

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/wilhasse/innodb-go/btr"
	"github.com/wilhasse/innodb-go/dict"
	"github.com/wilhasse/innodb-go/fil"
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
	Unique    bool
	Table     *TableSchema
}

// Table holds schema and storage.
type Table struct {
	ID      uint64
	Schema  *TableSchema
	Store   *row.Store
	SpaceID uint32
	Index   *dict.Index
}

// Database holds tables.
type Database struct {
	Name   string
	Tables map[string]*Table
}

var (
	schemaMu    sync.Mutex
	databases   = map[string]*Database{}
	nextTableID uint64
)

func resetSchemaState() {
	schemaMu.Lock()
	defer schemaMu.Unlock()
	for _, db := range databases {
		for _, table := range db.Tables {
			if table != nil && table.Store != nil {
				_ = table.Store.CloseFile()
			}
		}
	}
	databases = map[string]*Database{}
	nextTableID = 0
}

// TableSchemaCreate initializes a table schema.
func TableSchemaCreate(name string, out **TableSchema, format TableFormat, pageSize int) ErrCode {
	if out == nil {
		return DB_ERROR
	}
	if !validTableName(name) {
		return DB_DATA_MISMATCH
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

// IndexSchemaSetUnique marks an index as unique.
func IndexSchemaSetUnique(index *IndexSchema) ErrCode {
	if index == nil {
		return DB_ERROR
	}
	index.Unique = true
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
	dictTableID, err := dict.DictHdrGetNewID(dict.DictHdrTableID)
	if err != nil {
		return DB_ERROR
	}
	id := dict.DulintToUint64(dictTableID)
	atomic.StoreUint64(&nextTableID, id)
	if tableID != nil {
		*tableID = id
	}
	primaryKey := -1
	primaryKeyPrefix := 0
	var primaryKeyFields []int
	var primaryKeyPrefixes []int
	if schema != nil {
		for _, idx := range schema.Indexes {
			if idx == nil || !idx.Clustered || len(idx.Columns) == 0 {
				continue
			}
			for j, colName := range idx.Columns {
				colName = strings.ToLower(colName)
				for i, col := range schema.Columns {
					if strings.ToLower(col.Name) == colName {
						primaryKeyFields = append(primaryKeyFields, i)
						prefix := 0
						if j < len(idx.Prefixes) {
							prefix = idx.Prefixes[j]
						}
						primaryKeyPrefixes = append(primaryKeyPrefixes, prefix)
						break
					}
				}
			}
			if len(primaryKeyFields) > 0 {
				break
			}
		}
	}
	if len(primaryKeyFields) == 1 {
		primaryKey = primaryKeyFields[0]
		primaryKeyPrefix = primaryKeyPrefixes[0]
	}
	store := row.NewStore(primaryKey)
	store.PrimaryKeyPrefix = primaryKeyPrefix
	store.PrimaryKeyFields = primaryKeyFields
	store.PrimaryKeyPrefixes = primaryKeyPrefixes
	spaceID := uint32(id + 1)
	if !fil.SpaceCreate(schema.Name, spaceID, 0, fil.SpaceTablespace) {
		return DB_ERROR
	}
	indexName := "PRIMARY"
	var clusteredSchema *IndexSchema
	for _, idx := range schema.Indexes {
		if idx != nil && idx.Clustered {
			clusteredSchema = idx
			if idx.Name != "" {
				indexName = idx.Name
			}
			break
		}
	}
	index := &dict.Index{Name: indexName, Clustered: true, SpaceID: spaceID}
	if clusteredSchema != nil {
		index.Fields = append([]string(nil), clusteredSchema.Columns...)
		index.Unique = clusteredSchema.Unique
	}
	indexID, err := dict.DictHdrGetNewID(dict.DictHdrIndexID)
	if err != nil {
		fil.SpaceDrop(spaceID)
		return DB_ERROR
	}
	index.ID = indexID
	btr.Create(index)
	if err := attachTableFile(store, schema.Name); err != DB_SUCCESS {
		btr.FreeRoot(index)
		fil.SpaceDrop(spaceID)
		return err
	}
	dictTable, err := buildDictTable(schema, spaceID, id, index)
	if err != nil {
		btr.FreeRoot(index)
		fil.SpaceDrop(spaceID)
		_ = store.DeleteFile()
		return DB_ERROR
	}
	if err := dict.DictPersistTableCreate(dictTable); err != nil {
		btr.FreeRoot(index)
		fil.SpaceDrop(spaceID)
		_ = store.DeleteFile()
		return DB_ERROR
	}
	db.Tables[strings.ToLower(schema.Name)] = &Table{ID: id, Schema: schema, Store: store, SpaceID: spaceID, Index: index}
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
	table := db.Tables[strings.ToLower(name)]
	if table == nil {
		return DB_TABLE_NOT_FOUND
	}
	if table.Store != nil {
		_ = table.Store.DeleteFile()
	}
	if table.Index != nil {
		btr.FreeRoot(table.Index)
	}
	if table.SpaceID != 0 {
		fil.SpaceDrop(table.SpaceID)
	}
	if dictTable := dict.DictTableGet(name); dictTable != nil {
		_ = dict.DictPersistTableDrop(dictTable)
	}
	delete(db.Tables, strings.ToLower(name))
	return DB_SUCCESS
}

// TableTruncate clears all rows in a table and returns a new table id.
func TableTruncate(name string, tableID *uint64) ErrCode {
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
	table := db.Tables[strings.ToLower(name)]
	if table == nil {
		return DB_TABLE_NOT_FOUND
	}
	if table.Store != nil {
		table.Store.Reset()
	}
	if table.Index != nil {
		btr.FreeButNotRoot(table.Index)
	}
	table.ID = atomic.AddUint64(&nextTableID, 1)
	if tableID != nil {
		*tableID = table.ID
	}
	return DB_SUCCESS
}

func encodeTableFlags(format TableFormat, pageSize int) uint32 {
	if pageSize < 0 {
		pageSize = 0
	}
	return uint32(format) | (uint32(pageSize) << 8)
}

func buildDictTable(schema *TableSchema, spaceID uint32, tableID uint64, clustered *dict.Index) (*dict.Table, error) {
	if schema == nil {
		return nil, errors.New("api: invalid schema")
	}
	table := dict.MemTableCreate(schema.Name, spaceID, len(schema.Columns), encodeTableFlags(schema.Format, schema.PageSize))
	table.ID = dict.DulintFromUint64(tableID)
	for _, col := range schema.Columns {
		dict.MemTableAddCol(table, col.Name, uint32(col.Type), uint32(col.Attr), col.Size)
	}
	if table.Indexes == nil {
		table.Indexes = make(map[string]*dict.Index)
	}
	addedCluster := false
	for _, idxSchema := range schema.Indexes {
		if idxSchema == nil {
			continue
		}
		idxName := idxSchema.Name
		if idxName == "" && idxSchema.Clustered {
			idxName = "PRIMARY"
		}
		if idxName == "" {
			continue
		}
		idx := &dict.Index{
			Name:      idxName,
			Fields:    append([]string(nil), idxSchema.Columns...),
			Unique:    idxSchema.Unique,
			Clustered: idxSchema.Clustered,
			SpaceID:   spaceID,
		}
		if idxSchema.Clustered && clustered != nil {
			idx.ID = clustered.ID
			idx.RootPage = clustered.RootPage
			addedCluster = true
		} else {
			idxID, err := dict.DictHdrGetNewID(dict.DictHdrIndexID)
			if err != nil {
				return nil, err
			}
			idx.ID = idxID
		}
		table.Indexes[idx.Name] = idx
	}
	if !addedCluster && clustered != nil {
		idx := &dict.Index{
			Name:      clustered.Name,
			ID:        clustered.ID,
			Fields:    append([]string(nil), clustered.Fields...),
			Unique:    clustered.Unique,
			Clustered: true,
			RootPage:  clustered.RootPage,
			SpaceID:   spaceID,
		}
		table.Indexes[idx.Name] = idx
	}
	return table, nil
}

func splitTableName(name string) (string, string) {
	parts := strings.Split(name, "/")
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

func validTableName(name string) bool {
	if strings.Count(name, "/") != 1 {
		return false
	}
	if strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") {
		return false
	}
	db, table := splitTableName(name)
	if db == "" || table == "" {
		return false
	}
	if db == "." || db == ".." || table == "." || table == ".." {
		return false
	}
	return true
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
