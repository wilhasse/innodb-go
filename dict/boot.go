package dict

import (
	"errors"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/ut"
)

// Dictionary header constants.
const (
	DictHdrSpace                   = 0
	DictHdrPageNo                  = 7
	DictHdrFirstID                 = 10
	DictHdrRowIDWriteMargin        = 256
	dictRootPageBase        uint32 = 1000
)

// Dict header ID kinds.
const (
	DictHdrRowID = iota
	DictHdrTableID
	DictHdrIndexID
	DictHdrMixID
)

// System table IDs.
var (
	DictTablesID   = newDulint(0, 1)
	DictColumnsID  = newDulint(0, 2)
	DictIndexesID  = newDulint(0, 3)
	DictFieldsID   = newDulint(0, 4)
	DictTableIDsID = newDulint(0, 5)
	DictIbufIDMin  = newDulint(0xFFFFFFFF, 0)
)

var errDictNotInitialized = errors.New("dict: system not initialized")

// DictInit initializes the dictionary system.
func DictInit() {
	DictSys = &System{
		Tables: make(map[string]*Table),
	}
}

// DictHdrGet returns the dictionary header.
func DictHdrGet() *Header {
	if DictSys == nil {
		DictInit()
	}
	return &DictSys.Header
}

// DictHdrGetNewID returns a new ID for table or index.
func DictHdrGetNewID(kind int) (ut.Dulint, error) {
	if DictSys == nil {
		return ut.Dulint{}, errDictNotInitialized
	}
	DictSys.mu.Lock()
	defer DictSys.mu.Unlock()

	switch kind {
	case DictHdrTableID:
		DictSys.Header.TableID = dulintAdd(DictSys.Header.TableID, 1)
		return DictSys.Header.TableID, nil
	case DictHdrIndexID:
		DictSys.Header.IndexID = dulintAdd(DictSys.Header.IndexID, 1)
		return DictSys.Header.IndexID, nil
	default:
		return ut.Dulint{}, errors.New("dict: unsupported id kind")
	}
}

// DictHdrFlushRowID writes the current row ID to the header.
func DictHdrFlushRowID() error {
	if DictSys == nil {
		return errDictNotInitialized
	}
	DictSys.mu.Lock()
	defer DictSys.mu.Unlock()
	DictSys.Header.RowID = DictSys.RowID
	return nil
}

// DictSysGetNewRowID returns a new row ID.
func DictSysGetNewRowID() (ut.Dulint, error) {
	if DictSys == nil {
		return ut.Dulint{}, errDictNotInitialized
	}
	DictSys.mu.Lock()
	defer DictSys.mu.Unlock()

	DictSys.RowID = dulintAdd(DictSys.RowID, 1)
	if dulintToUint64(DictSys.RowID)%DictHdrRowIDWriteMargin == 0 {
		DictSys.Header.RowID = DictSys.RowID
	}
	return DictSys.RowID, nil
}

// DictBoot initializes the dictionary cache and system tables.
func DictBoot() {
	if DictSys == nil {
		DictInit()
	}

	DictSys.mu.Lock()
	defer DictSys.mu.Unlock()

	DictSys.RowID = dulintAdd(dulintAlignUp(DictSys.Header.RowID, DictHdrRowIDWriteMargin), DictHdrRowIDWriteMargin)

	createSysTables()
	initSysRows()
}

// DictCreate creates the dictionary header and boots the dictionary.
func DictCreate() {
	if DictSys == nil {
		DictInit()
	}
	DictSys.mu.Lock()
	dictHdrCreate()
	DictSys.mu.Unlock()

	DictBoot()
}

func dictHdrCreate() {
	header := &DictSys.Header
	header.RowID = newDulint(0, DictHdrFirstID)
	header.TableID = newDulint(0, DictHdrFirstID)
	header.IndexID = newDulint(0, DictHdrFirstID)
	header.MixID = newDulint(0, DictHdrFirstID)

	header.TablesRoot = dictRootPageBase
	header.TableIDsRoot = dictRootPageBase + 1
	header.ColumnsRoot = dictRootPageBase + 2
	header.IndexesRoot = dictRootPageBase + 3
	header.FieldsRoot = dictRootPageBase + 4
}

func createSysTables() {
	DictSys.Tables = make(map[string]*Table)

	sysTables := newTable("SYS_TABLES", DictHdrSpace, 8)
	addColumn(sysTables, "NAME", data.DataBinary)
	addColumn(sysTables, "ID", data.DataBinary)
	addColumn(sysTables, "N_COLS", data.DataInt)
	addColumn(sysTables, "TYPE", data.DataInt)
	addColumn(sysTables, "MIX_ID", data.DataBinary)
	addColumn(sysTables, "MIX_LEN", data.DataInt)
	addColumn(sysTables, "CLUSTER_NAME", data.DataBinary)
	addColumn(sysTables, "SPACE", data.DataInt)
	sysTables.ID = DictTablesID
	addTable(sysTables)
	DictSys.SysTables = sysTables

	addIndex(sysTables, "CLUST_IND", DictTablesID, true, true, DictSys.Header.TablesRoot, "NAME")
	addIndex(sysTables, "ID_IND", DictTableIDsID, true, false, DictSys.Header.TableIDsRoot, "ID")

	sysColumns := newTable("SYS_COLUMNS", DictHdrSpace, 7)
	addColumn(sysColumns, "TABLE_ID", data.DataBinary)
	addColumn(sysColumns, "POS", data.DataInt)
	addColumn(sysColumns, "NAME", data.DataBinary)
	addColumn(sysColumns, "MTYPE", data.DataInt)
	addColumn(sysColumns, "PRTYPE", data.DataInt)
	addColumn(sysColumns, "LEN", data.DataInt)
	addColumn(sysColumns, "PREC", data.DataInt)
	sysColumns.ID = DictColumnsID
	addTable(sysColumns)
	DictSys.SysColumns = sysColumns

	addIndex(sysColumns, "CLUST_IND", DictColumnsID, true, true, DictSys.Header.ColumnsRoot, "TABLE_ID", "POS")

	sysIndexes := newTable("SYS_INDEXES", DictHdrSpace, 7)
	addColumn(sysIndexes, "TABLE_ID", data.DataBinary)
	addColumn(sysIndexes, "ID", data.DataBinary)
	addColumn(sysIndexes, "NAME", data.DataBinary)
	addColumn(sysIndexes, "N_FIELDS", data.DataInt)
	addColumn(sysIndexes, "TYPE", data.DataInt)
	addColumn(sysIndexes, "SPACE", data.DataInt)
	addColumn(sysIndexes, "PAGE_NO", data.DataInt)
	sysIndexes.ID = DictIndexesID
	addTable(sysIndexes)
	DictSys.SysIndexes = sysIndexes

	addIndex(sysIndexes, "CLUST_IND", DictIndexesID, true, true, DictSys.Header.IndexesRoot, "TABLE_ID", "ID")

	sysFields := newTable("SYS_FIELDS", DictHdrSpace, 3)
	addColumn(sysFields, "INDEX_ID", data.DataBinary)
	addColumn(sysFields, "POS", data.DataInt)
	addColumn(sysFields, "COL_NAME", data.DataBinary)
	sysFields.ID = DictFieldsID
	addTable(sysFields)
	DictSys.SysFields = sysFields

	addIndex(sysFields, "CLUST_IND", DictFieldsID, true, true, DictSys.Header.FieldsRoot, "INDEX_ID", "POS")
}

func newTable(name string, space uint32, nCols int) *Table {
	if nCols < 0 {
		nCols = 0
	}
	return &Table{
		Name:    name,
		Space:   space,
		Columns: make([]Column, 0, nCols),
		Indexes: make(map[string]*Index),
	}
}

func addColumn(table *Table, name string, mtype uint32) {
	if table == nil {
		return
	}
	table.Columns = append(table.Columns, Column{
		Name: name,
		Type: data.DataType{MType: mtype},
	})
}

func addTable(table *Table) {
	if table == nil {
		return
	}
	DictSys.Tables[table.Name] = table
}

func addIndex(table *Table, name string, id ut.Dulint, unique bool, clustered bool, rootPage uint32, fields ...string) *Index {
	if table == nil {
		return nil
	}
	idx := &Index{
		Name:      name,
		ID:        id,
		Fields:    append([]string(nil), fields...),
		Unique:    unique,
		Clustered: clustered,
		RootPage:  rootPage,
	}
	table.Indexes[name] = idx
	return idx
}

func newDulint(high, low uint32) ut.Dulint {
	return ut.Dulint{High: ut.Ulint(high), Low: ut.Ulint(low)}
}

func dulintToUint64(d ut.Dulint) uint64 {
	return (uint64(d.High) << 32) | uint64(d.Low)
}

func dulintAdd(d ut.Dulint, inc uint64) ut.Dulint {
	value := dulintToUint64(d) + inc
	return newDulint(uint32(value>>32), uint32(value))
}

func dulintAlignUp(d ut.Dulint, align uint64) ut.Dulint {
	value := dulintToUint64(d)
	if align == 0 {
		return d
	}
	rem := value % align
	if rem == 0 {
		return d
	}
	value += align - rem
	return newDulint(uint32(value>>32), uint32(value))
}
