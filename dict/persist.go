package dict

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/wilhasse/innodb-go/data"
	ibos "github.com/wilhasse/innodb-go/os"
	"github.com/wilhasse/innodb-go/rec"
)

const dictFileName = "ib_dict.sys"

var dictDataDir string

// SetDataDir configures where dictionary metadata is persisted.
func SetDataDir(dir string) {
	dictDataDir = dir
}

func dictFilePath() string {
	dir := dictDataDir
	if dir == "" {
		dir = "."
	}
	return filepath.Join(dir, dictFileName)
}

type sysPersist struct {
	Header  Header
	Tables  [][]byte
	Columns [][]byte
	Indexes [][]byte
	Fields  [][]byte
}

// DictPersist writes the current SYS_* rows to disk.
func DictPersist() error {
	if DictSys == nil {
		return errors.New("dict: not initialized")
	}
	DictSys.mu.Lock()
	payload := buildPersistPayload()
	DictSys.mu.Unlock()

	path := dictFilePath()
	if err := ibos.FileCreateSubdirsIfNeeded(path); err != nil {
		return err
	}
	file, err := ibos.FileCreateSimple(path, ibos.FileOverwrite, ibos.FileReadWrite)
	if err != nil {
		return err
	}
	defer func() {
		_ = ibos.FileClose(file)
	}()
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(payload); err != nil {
		return err
	}
	_, err = ibos.FileWriteAt(file, buf.Bytes(), 0)
	return err
}

// DictPersistTableCreate records table metadata in SYS_* rows and persists it.
func DictPersistTableCreate(table *Table) error {
	if table == nil || table.Name == "" {
		return ErrInvalidName
	}
	if DictSys == nil {
		DictBootstrap()
	}
	DictSys.mu.Lock()
	if DictSys.Tables == nil {
		DictSys.Tables = make(map[string]*Table)
	}
	removeTableSysRows(table)
	addTableSysRows(table)
	dedupeSysRows()
	DictSys.Tables[table.Name] = table
	DictSys.mu.Unlock()
	return DictPersist()
}

// DictPersistTableDrop removes table metadata from SYS_* rows and persists it.
func DictPersistTableDrop(table *Table) error {
	if table == nil || table.Name == "" {
		return ErrInvalidName
	}
	if DictSys == nil {
		return ErrTableNotFound
	}
	DictSys.mu.Lock()
	removeTableSysRows(table)
	dedupeSysRows()
	delete(DictSys.Tables, table.Name)
	DictSys.mu.Unlock()
	return DictPersist()
}

func buildPersistPayload() *sysPersist {
	payload := &sysPersist{Header: DictSys.Header}
	payload.Tables = encodeRows(DictSys.SysRows.Tables)
	payload.Columns = encodeRows(DictSys.SysRows.Columns)
	payload.Indexes = encodeRows(DictSys.SysRows.Indexes)
	payload.Fields = encodeRows(DictSys.SysRows.Fields)
	return payload
}

func encodeRows(rows []*data.Tuple) [][]byte {
	encoded := make([][]byte, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		bytes, err := rec.EncodeVar(row, nil, 0)
		if err != nil {
			continue
		}
		encoded = append(encoded, bytes)
	}
	return encoded
}

func addTableSysRows(table *Table) {
	if DictSys == nil || table == nil {
		return
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

func removeTableSysRows(table *Table) {
	if DictSys == nil || table == nil {
		return
	}
	tableID := DulintToUint64(table.ID)
	indexIDs := make(map[uint64]struct{})
	for _, idx := range table.Indexes {
		if idx == nil {
			continue
		}
		indexIDs[DulintToUint64(idx.ID)] = struct{}{}
	}

	DictSys.SysRows.Tables = filterRows(DictSys.SysRows.Tables, func(row *data.Tuple) bool {
		if row == nil || len(row.Fields) < 2 {
			return true
		}
		name := string(row.Fields[0].Data)
		if name == table.Name {
			return false
		}
		id, ok := tupleFieldUint64(row, 1)
		return !ok || id != tableID
	})
	DictSys.SysRows.Columns = filterRows(DictSys.SysRows.Columns, func(row *data.Tuple) bool {
		id, ok := tupleFieldUint64(row, 0)
		return !ok || id != tableID
	})
	DictSys.SysRows.Indexes = filterRows(DictSys.SysRows.Indexes, func(row *data.Tuple) bool {
		id, ok := tupleFieldUint64(row, 0)
		return !ok || id != tableID
	})
	DictSys.SysRows.Fields = filterRows(DictSys.SysRows.Fields, func(row *data.Tuple) bool {
		id, ok := tupleFieldUint64(row, 0)
		if !ok {
			return true
		}
		if _, exists := indexIDs[id]; exists {
			return false
		}
		return true
	})
}

func dedupeSysRows() {
	if DictSys == nil {
		return
	}
	DictSys.SysRows.Tables = dedupeTableRows(DictSys.SysRows.Tables)
	DictSys.SysRows.Columns = dedupeColumnRows(DictSys.SysRows.Columns)
	DictSys.SysRows.Indexes = dedupeIndexRows(DictSys.SysRows.Indexes)
	DictSys.SysRows.Fields = dedupeFieldRows(DictSys.SysRows.Fields)
}

func dedupeTableRows(rows []*data.Tuple) []*data.Tuple {
	seen := make(map[string]struct{})
	dst := rows[:0]
	for _, row := range rows {
		if row == nil {
			continue
		}
		name, _ := tupleFieldString(row, 0)
		key := "name:" + name
		if name == "" {
			if id, ok := tupleFieldUint64(row, 1); ok {
				key = fmt.Sprintf("id:%d", id)
			} else {
				key = fmt.Sprintf("row:%p", row)
			}
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		dst = append(dst, row)
	}
	return dst
}

func dedupeColumnRows(rows []*data.Tuple) []*data.Tuple {
	seen := make(map[string]struct{})
	dst := rows[:0]
	for _, row := range rows {
		if row == nil {
			continue
		}
		tableID, okID := tupleFieldUint64(row, 0)
		pos, okPos := tupleFieldUint32(row, 1)
		key := fmt.Sprintf("row:%p", row)
		if okID && okPos {
			key = fmt.Sprintf("%d:%d", tableID, pos)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		dst = append(dst, row)
	}
	return dst
}

func dedupeIndexRows(rows []*data.Tuple) []*data.Tuple {
	seen := make(map[string]struct{})
	dst := rows[:0]
	for _, row := range rows {
		if row == nil {
			continue
		}
		tableID, okTable := tupleFieldUint64(row, 0)
		indexID, okIndex := tupleFieldUint64(row, 1)
		key := fmt.Sprintf("row:%p", row)
		if okTable && okIndex {
			key = fmt.Sprintf("%d:%d", tableID, indexID)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		dst = append(dst, row)
	}
	return dst
}

func dedupeFieldRows(rows []*data.Tuple) []*data.Tuple {
	seen := make(map[string]struct{})
	dst := rows[:0]
	for _, row := range rows {
		if row == nil {
			continue
		}
		indexID, okIndex := tupleFieldUint64(row, 0)
		pos, okPos := tupleFieldUint32(row, 1)
		key := fmt.Sprintf("row:%p", row)
		if okIndex && okPos {
			key = fmt.Sprintf("%d:%d", indexID, pos)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		dst = append(dst, row)
	}
	return dst
}

func filterRows(rows []*data.Tuple, keep func(row *data.Tuple) bool) []*data.Tuple {
	if len(rows) == 0 {
		return rows
	}
	dst := rows[:0]
	for _, row := range rows {
		if keep(row) {
			dst = append(dst, row)
		}
	}
	return dst
}

func tupleFieldUint64(row *data.Tuple, idx int) (uint64, bool) {
	if row == nil || idx < 0 || idx >= len(row.Fields) {
		return 0, false
	}
	field := row.Fields[idx]
	if len(field.Data) < 8 {
		return 0, false
	}
	return readUint64(field.Data), true
}

func tupleFieldUint32(row *data.Tuple, idx int) (uint32, bool) {
	if row == nil || idx < 0 || idx >= len(row.Fields) {
		return 0, false
	}
	field := row.Fields[idx]
	if len(field.Data) < 4 {
		return 0, false
	}
	return readUint32(field.Data), true
}

func tupleFieldString(row *data.Tuple, idx int) (string, bool) {
	if row == nil || idx < 0 || idx >= len(row.Fields) {
		return "", false
	}
	field := row.Fields[idx]
	return string(field.Data), true
}

func readUint64(buf []byte) uint64 {
	if len(buf) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(buf[:8])
}

func readUint32(buf []byte) uint32 {
	if len(buf) < 4 {
		return 0
	}
	return binary.BigEndian.Uint32(buf[:4])
}
