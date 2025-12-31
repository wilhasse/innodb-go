package dict

import (
	"bytes"
	"encoding/gob"
	"errors"
	"strings"

	"github.com/wilhasse/innodb-go/data"
	ibos "github.com/wilhasse/innodb-go/os"
	"github.com/wilhasse/innodb-go/rec"
)

const (
	sysTablesFields  = 8
	sysColumnsFields = 7
	sysIndexesFields = 7
	sysFieldsFields  = 3
)

func loadPersisted() (*sysPersist, error) {
	path := dictFilePath()
	exists, err := ibos.FileExists(path)
	if err != nil || !exists {
		return nil, errors.New("dict: no persisted metadata")
	}
	file, err := ibos.FileCreateSimple(path, ibos.FileOpen, ibos.FileReadOnly)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = ibos.FileClose(file)
	}()
	size, err := ibos.FileSize(file)
	if err != nil || size == 0 {
		return nil, err
	}
	buf := make([]byte, size)
	if _, err := ibos.FileReadAt(file, buf, 0); err != nil {
		return nil, err
	}
	decoder := gob.NewDecoder(bytes.NewReader(buf))
	var payload sysPersist
	if err := decoder.Decode(&payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

func decodeRows(encoded [][]byte, nFields int) []*data.Tuple {
	rows := make([]*data.Tuple, 0, len(encoded))
	for _, rowBytes := range encoded {
		if len(rowBytes) == 0 {
			continue
		}
		row, err := rec.DecodeVar(rowBytes, nFields, 0)
		if err != nil {
			continue
		}
		rows = append(rows, row)
	}
	return rows
}

func rebuildFromSysRows() {
	if DictSys == nil {
		return
	}
	createSysTables()
	tableByID := make(map[uint64]*Table)
	for _, row := range DictSys.SysRows.Tables {
		name, ok := tupleFieldString(row, 0)
		if !ok || strings.HasPrefix(strings.ToUpper(name), "SYS_") {
			continue
		}
		id, ok := tupleFieldUint64(row, 1)
		if !ok {
			continue
		}
		nCols, _ := tupleFieldUint32(row, 2)
		flags, _ := tupleFieldUint32(row, 3)
		space, _ := tupleFieldUint32(row, 7)
		table := &Table{
			Name:    name,
			ID:      DulintFromUint64(id),
			Space:   space,
			Flags:   flags,
			Columns: make([]Column, nCols),
			Indexes: make(map[string]*Index),
		}
		DictSys.Tables[name] = table
		tableByID[id] = table
	}

	for _, row := range DictSys.SysRows.Columns {
		tableID, ok := tupleFieldUint64(row, 0)
		if !ok {
			continue
		}
		table := tableByID[tableID]
		if table == nil {
			continue
		}
		pos, ok := tupleFieldUint32(row, 1)
		if !ok {
			continue
		}
		colName, ok := tupleFieldString(row, 2)
		if !ok {
			continue
		}
		mtype, _ := tupleFieldUint32(row, 3)
		prtype, _ := tupleFieldUint32(row, 4)
		length, _ := tupleFieldUint32(row, 5)
		idx := int(pos)
		if idx < 0 {
			continue
		}
		if idx >= len(table.Columns) {
			cols := make([]Column, idx+1)
			copy(cols, table.Columns)
			table.Columns = cols
		}
		table.Columns[idx] = Column{
			Name: colName,
			Type: data.DataType{MType: mtype, PrType: prtype, Len: length},
		}
	}

	indexByID := make(map[uint64]*Index)
	for _, row := range DictSys.SysRows.Indexes {
		tableID, ok := tupleFieldUint64(row, 0)
		if !ok {
			continue
		}
		table := tableByID[tableID]
		if table == nil {
			continue
		}
		indexID, ok := tupleFieldUint64(row, 1)
		if !ok {
			continue
		}
		name, ok := tupleFieldString(row, 2)
		if !ok {
			continue
		}
		nFields, _ := tupleFieldUint32(row, 3)
		typ, _ := tupleFieldUint32(row, 4)
		space, _ := tupleFieldUint32(row, 5)
		root, _ := tupleFieldUint32(row, 6)
		idx := &Index{
			Name:      name,
			ID:        DulintFromUint64(indexID),
			Fields:    make([]string, nFields),
			Unique:    typ&DictIndexUnique != 0,
			Clustered: typ&DictIndexClustered != 0,
			RootPage:  root,
			SpaceID:   space,
		}
		table.Indexes[name] = idx
		indexByID[indexID] = idx
	}

	for _, row := range DictSys.SysRows.Fields {
		indexID, ok := tupleFieldUint64(row, 0)
		if !ok {
			continue
		}
		idx := indexByID[indexID]
		if idx == nil {
			continue
		}
		pos, ok := tupleFieldUint32(row, 1)
		if !ok {
			continue
		}
		name, ok := tupleFieldString(row, 2)
		if !ok {
			continue
		}
		idxPos := int(pos)
		if idxPos < 0 {
			continue
		}
		if idxPos >= len(idx.Fields) {
			fields := make([]string, idxPos+1)
			copy(fields, idx.Fields)
			idx.Fields = fields
		}
		idx.Fields[idxPos] = name
	}
}
