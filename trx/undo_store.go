package trx

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"sync"

	ibos "github.com/wilhasse/innodb-go/os"
)

const undoLogFileName = "ib_undo.log"

// UndoStore persists undo records to a log file.
type UndoStore struct {
	mu     sync.Mutex
	file   ibos.File
	path   string
	offset int64
}

var (
	undoStore     *UndoStore
	UndoRecovered []UndoRecord
)

// UndoStoreInit opens the undo log file in the provided data directory.
func UndoStoreInit(dir string) error {
	path := undoLogPath(dir)
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
	size, err := ibos.FileSize(file)
	if err != nil {
		_ = ibos.FileClose(file)
		return err
	}
	undoStore = &UndoStore{file: file, path: path, offset: size}
	UndoRecovered = nil
	return nil
}

// UndoStoreClose closes the undo log file.
func UndoStoreClose() error {
	if undoStore == nil {
		return nil
	}
	undoStore.mu.Lock()
	file := undoStore.file
	undoStore.file = nil
	undoStore.mu.Unlock()
	undoStore = nil
	return ibos.FileClose(file)
}

// UndoStoreAppend appends an undo record to the log.
func UndoStoreAppend(rec UndoRecord) error {
	if undoStore == nil || undoStore.file == nil {
		return nil
	}
	bytes := EncodeUndoRecord(&rec)
	if len(bytes) == 0 {
		return nil
	}
	buf := make([]byte, 4+len(bytes))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(bytes)))
	copy(buf[4:], bytes)
	undoStore.mu.Lock()
	defer undoStore.mu.Unlock()
	_, err := ibos.FileWriteAt(undoStore.file, buf, undoStore.offset)
	if err != nil {
		return err
	}
	undoStore.offset += int64(len(buf))
	return nil
}

// UndoStoreLoad reads all persisted undo records.
func UndoStoreLoad() ([]UndoRecord, error) {
	if undoStore == nil || undoStore.file == nil {
		return nil, nil
	}
	size, err := ibos.FileSize(undoStore.file)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, nil
	}
	buf := make([]byte, size)
	if _, err := ibos.FileReadAt(undoStore.file, buf, 0); err != nil {
		return nil, err
	}
	records := make([]UndoRecord, 0)
	for off := 0; off+4 <= len(buf); {
		length := int(binary.BigEndian.Uint32(buf[off : off+4]))
		off += 4
		if length <= 0 || off+length > len(buf) {
			break
		}
		recBytes := buf[off : off+length]
		off += length
		rec, err := DecodeUndoRecord(recBytes)
		if err != nil {
			return records, err
		}
		records = append(records, *rec)
	}
	return records, nil
}

// UndoStoreRecover loads persisted undo records into the rollback segment system.
func UndoStoreRecover() error {
	records, err := UndoStoreLoad()
	if err != nil {
		return err
	}
	UndoRecovered = records
	if len(records) == 0 {
		return nil
	}
	rseg := RsegGetOnID(1)
	if rseg == nil {
		rseg = RsegCreate(1, len(records)+16)
	}
	for _, rec := range records {
		switch undoLogTypeForRecord(rec.Type) {
		case UndoLogInsert:
			_ = rseg.AddInsertUndo(rec)
		default:
			_ = rseg.AddUpdateUndo(rec)
		}
	}
	return nil
}

// UndoRecoveredCount returns the number of recovered undo records.
func UndoRecoveredCount() int {
	return len(UndoRecovered)
}

func undoLogPath(dir string) string {
	if dir == "" {
		return undoLogFileName
	}
	return filepath.Join(dir, undoLogFileName)
}

var errUndoStoreNotInitialized = errors.New("trx: undo store not initialized")

// UndoStorePath exposes the current undo log path for diagnostics.
func UndoStorePath() (string, error) {
	if undoStore == nil {
		return "", errUndoStoreNotInitialized
	}
	undoStore.mu.Lock()
	defer undoStore.mu.Unlock()
	return undoStore.path, nil
}
