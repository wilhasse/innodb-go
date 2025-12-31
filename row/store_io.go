package row

import (
	"encoding/binary"
	"errors"
	"sort"

	"github.com/wilhasse/innodb-go/btr"
	"github.com/wilhasse/innodb-go/data"
	ibos "github.com/wilhasse/innodb-go/os"
)

const (
	storeOpInsert byte = 1
	storeOpUpdate byte = 2
	storeOpDelete byte = 3
)

func (store *Store) AttachFile(path string) error {
	if store == nil || path == "" {
		return errors.New("row: invalid file path")
	}
	if err := ibos.FileCreateSubdirsIfNeeded(path); err != nil {
		return err
	}
	exists, err := ibos.FileExists(path)
	if err != nil {
		return err
	}
	createMode := ibos.FileCreate
	if exists {
		createMode = ibos.FileOpen
	}
	file, err := ibos.FileCreateSimple(path, createMode, ibos.FileReadWrite)
	if err != nil {
		return err
	}
	store.file = file
	store.filePath = path
	if exists {
		if err := store.loadFromFile(); err != nil {
			_ = ibos.FileClose(file)
			store.file = nil
			store.filePath = ""
			return err
		}
	}
	if size, err := ibos.FileSize(file); err == nil {
		store.fileOffset = size
	} else {
		store.fileOffset = 0
	}
	return nil
}

func (store *Store) CloseFile() error {
	if store == nil {
		return nil
	}
	err := ibos.FileClose(store.file)
	store.file = nil
	store.filePath = ""
	store.fileOffset = 0
	return err
}

func (store *Store) DeleteFile() error {
	if store == nil {
		return nil
	}
	path := store.filePath
	_ = ibos.FileClose(store.file)
	store.file = nil
	store.filePath = ""
	store.fileOffset = 0
	if path == "" {
		return nil
	}
	return ibos.FileDelete(path)
}

func (store *Store) TruncateFile() error {
	if store == nil {
		return nil
	}
	path := store.filePath
	if path == "" {
		return nil
	}
	_ = ibos.FileClose(store.file)
	file, err := ibos.FileCreateSimple(path, ibos.FileOverwrite, ibos.FileReadWrite)
	if err != nil {
		return err
	}
	store.file = file
	store.filePath = path
	store.fileOffset = 0
	return nil
}

func (store *Store) appendLog(op byte, key, value []byte) {
	if store == nil || store.file == nil {
		return
	}
	var header [9]byte
	header[0] = op
	binary.BigEndian.PutUint32(header[1:5], uint32(len(key)))
	binary.BigEndian.PutUint32(header[5:9], uint32(len(value)))
	buf := make([]byte, 0, len(header)+len(key)+len(value))
	buf = append(buf, header[:]...)
	buf = append(buf, key...)
	buf = append(buf, value...)
	_, _ = ibos.FileWriteAt(store.file, buf, store.fileOffset)
	store.fileOffset += int64(len(buf))
}

type logEntry struct {
	op    byte
	key   []byte
	value []byte
}

func (store *Store) loadFromFile() error {
	if store == nil || store.file == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	size, err := ibos.FileSize(store.file)
	if err != nil || size == 0 {
		return err
	}
	buf := make([]byte, size)
	if _, err := ibos.FileReadAt(store.file, buf, 0); err != nil {
		return err
	}
	entries := parseLogEntries(buf)
	store.applyLogEntries(entries)
	return nil
}

func parseLogEntries(buf []byte) []logEntry {
	entries := make([]logEntry, 0)
	for off := 0; off+9 <= len(buf); {
		op := buf[off]
		keyLen := int(binary.BigEndian.Uint32(buf[off+1 : off+5]))
		valLen := int(binary.BigEndian.Uint32(buf[off+5 : off+9]))
		off += 9
		if keyLen < 0 || valLen < 0 || off+keyLen+valLen > len(buf) {
			break
		}
		key := append([]byte(nil), buf[off:off+keyLen]...)
		off += keyLen
		value := append([]byte(nil), buf[off:off+valLen]...)
		off += valLen
		entries = append(entries, logEntry{op: op, key: key, value: value})
	}
	return entries
}

func (store *Store) applyLogEntries(entries []logEntry) {
	if store == nil {
		return
	}
	store.Rows = nil
	store.Tree = btr.NewTree(storeTreeOrder, CompareKeys)
	store.rowsByID = make(map[uint64]*data.Tuple)
	store.idByRow = make(map[*data.Tuple]uint64)
	keysByID := make(map[uint64][]byte)
	var maxID uint64

	for _, entry := range entries {
		switch entry.op {
		case storeOpInsert, storeOpUpdate:
			id, tuple, err := decodeRowValue(entry.value)
			if err != nil || tuple == nil {
				continue
			}
			if id > maxID {
				maxID = id
			}
			if oldKey, ok := keysByID[id]; ok {
				store.Tree.Delete(oldKey)
			}
			keysByID[id] = entry.key
			store.rowsByID[id] = tuple
			store.Tree.Insert(entry.key, encodeRowValue(id, tuple))
		case storeOpDelete:
			id := lookupRowID(store, entry.key)
			if id == 0 {
				continue
			}
			row := store.rowsByID[id]
			delete(store.rowsByID, id)
			if row != nil {
				delete(store.idByRow, row)
			}
			delete(keysByID, id)
			store.Tree.Delete(entry.key)
		}
	}

	ids := make([]uint64, 0, len(store.rowsByID))
	for id := range store.rowsByID {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		row := store.rowsByID[id]
		store.Rows = append(store.Rows, row)
		store.idByRow[row] = id
	}
	if maxID > 0 {
		store.nextRowID = maxID + 1
	} else {
		store.nextRowID = 1
	}
}

func lookupRowID(store *Store, key []byte) uint64 {
	if store == nil || store.Tree == nil {
		return 0
	}
	value, ok := store.Tree.Search(key)
	if !ok {
		return 0
	}
	id, _ := DecodeRowID(value)
	return id
}

func decodeRowValue(value []byte) (uint64, *data.Tuple, error) {
	if len(value) < 8 {
		return 0, nil, errors.New("row: value too short")
	}
	id := binary.BigEndian.Uint64(value[:8])
	tuple, err := decodeVarTuple(value[8:])
	if err != nil {
		return id, nil, err
	}
	return id, tuple, nil
}

func decodeVarTuple(buf []byte) (*data.Tuple, error) {
	fields := make([]data.Field, 0)
	for pos := 0; pos < len(buf); {
		if pos+3 > len(buf) {
			return nil, errors.New("row: truncated field")
		}
		nullFlag := buf[pos]
		pos++
		length := int(binary.BigEndian.Uint16(buf[pos : pos+2]))
		pos += 2
		field := data.Field{}
		if nullFlag != 0 {
			data.FieldSetNull(&field)
			fields = append(fields, field)
			continue
		}
		if pos+length > len(buf) {
			return nil, errors.New("row: truncated field data")
		}
		dataBytes := append([]byte(nil), buf[pos:pos+length]...)
		data.FieldSetData(&field, dataBytes, uint32(length))
		fields = append(fields, field)
		pos += length
	}
	tuple := data.NewTuple(len(fields))
	copy(tuple.Fields, fields)
	return tuple, nil
}
