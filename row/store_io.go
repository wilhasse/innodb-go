package row

import (
	"encoding/binary"
	"errors"

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
	file, err := ibos.FileCreateSimple(path, ibos.FileOverwrite, ibos.FileReadWrite)
	if err != nil {
		return err
	}
	store.file = file
	store.filePath = path
	store.fileOffset = 0
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
