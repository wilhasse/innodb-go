package dict

import "github.com/wilhasse/innodb-go/data"

// SysRows holds SYS_* table row data.
type SysRows struct {
	Tables  []*data.Tuple
	Columns []*data.Tuple
	Indexes []*data.Tuple
	Fields  []*data.Tuple
}

// DictBootstrap initializes the dictionary and system table rows.
func DictBootstrap() {
	DictInit()
	if sysPersister != nil {
		DictSys.mu.Lock()
		if payload, err := loadPersisted(); err == nil && payload != nil {
			DictSys.Header = payload.Header
		} else {
			dictHdrCreate()
		}
		createSysTables()
		DictSys.mu.Unlock()
		rows, err := sysPersister.LoadSysRows()
		if err == nil && !sysRowsEmpty(rows) {
			DictSys.mu.Lock()
			DictSys.SysRows = rows
			dedupeSysRows()
			updateHeaderFromSysRows()
			DictSys.RowID = dulintAdd(dulintAlignUp(DictSys.Header.RowID, DictHdrRowIDWriteMargin), DictHdrRowIDWriteMargin)
			DictSys.mu.Unlock()
			rebuildFromSysRows()
			return
		}
		DictSys.mu.Lock()
		DictSys.RowID = dulintAdd(dulintAlignUp(DictSys.Header.RowID, DictHdrRowIDWriteMargin), DictHdrRowIDWriteMargin)
		initSysRows()
		DictSys.mu.Unlock()
		_ = sysPersister.PersistSysRows(DictSys.SysRows)
		return
	}
	if payload, err := loadPersisted(); err == nil && payload != nil {
		DictSys.mu.Lock()
		DictSys.Header = payload.Header
		DictSys.RowID = dulintAdd(dulintAlignUp(DictSys.Header.RowID, DictHdrRowIDWriteMargin), DictHdrRowIDWriteMargin)
		DictSys.SysRows.Tables = decodeRows(payload.Tables, sysTablesFields)
		DictSys.SysRows.Columns = decodeRows(payload.Columns, sysColumnsFields)
		DictSys.SysRows.Indexes = decodeRows(payload.Indexes, sysIndexesFields)
		DictSys.SysRows.Fields = decodeRows(payload.Fields, sysFieldsFields)
		dedupeSysRows()
		DictSys.mu.Unlock()
		rebuildFromSysRows()
		return
	}
	DictCreate()
}

func initSysRows() {
	if DictSys == nil {
		return
	}
	DictSys.SysRows = SysRows{}
	sysTables := []*Table{
		DictSys.SysTables,
		DictSys.SysColumns,
		DictSys.SysIndexes,
		DictSys.SysFields,
	}
	for _, table := range sysTables {
		if table == nil {
			continue
		}
		DictSys.SysRows.Tables = append(DictSys.SysRows.Tables, CreateSysTablesTuple(table))
		for i := range table.Columns {
			DictSys.SysRows.Columns = append(DictSys.SysRows.Columns, CreateSysColumnsTuple(table, i))
		}
		for _, idx := range table.Indexes {
			if idx == nil {
				continue
			}
			DictSys.SysRows.Indexes = append(DictSys.SysRows.Indexes, CreateSysIndexesTuple(table, idx))
			for i := range idx.Fields {
				DictSys.SysRows.Fields = append(DictSys.SysRows.Fields, CreateSysFieldsTuple(idx, i))
			}
		}
	}
	dedupeSysRows()
}

func sysRowsEmpty(rows SysRows) bool {
	return len(rows.Tables) == 0 &&
		len(rows.Columns) == 0 &&
		len(rows.Indexes) == 0 &&
		len(rows.Fields) == 0
}

func updateHeaderFromSysRows() {
	if DictSys == nil {
		return
	}
	var maxTableID uint64
	for _, row := range DictSys.SysRows.Tables {
		if id, ok := tupleFieldUint64(row, 1); ok && id > maxTableID {
			maxTableID = id
		}
	}
	if maxTableID > 0 {
		DictSys.Header.TableID = DulintFromUint64(maxTableID)
	}
	var maxIndexID uint64
	for _, row := range DictSys.SysRows.Indexes {
		if id, ok := tupleFieldUint64(row, 1); ok && id > maxIndexID {
			maxIndexID = id
		}
	}
	if maxIndexID > 0 {
		DictSys.Header.IndexID = DulintFromUint64(maxIndexID)
	}
}
