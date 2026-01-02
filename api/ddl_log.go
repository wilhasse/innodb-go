package api

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"

	"github.com/wilhasse/innodb-go/dict"
	"github.com/wilhasse/innodb-go/fil"
	ibos "github.com/wilhasse/innodb-go/os"
)

const ddlLogFileName = "ib_ddl.log"

const (
	ddlOpCreate = "create"
	ddlOpDrop   = "drop"
	ddlOpRename = "rename"
)

type ddlLogEntry struct {
	Op       string `json:"op"`
	Table    string `json:"table"`
	NewTable string `json:"new_table,omitempty"`
}

var ddlLogMu sync.Mutex

func ddlLogPath() string {
	return filepath.Join(dataHomeDir(), ddlLogFileName)
}

func writeDDLLogEntry(entry ddlLogEntry) error {
	if entry.Op == "" || entry.Table == "" {
		return errors.New("api: invalid ddl log entry")
	}
	ddlLogMu.Lock()
	defer ddlLogMu.Unlock()
	path := ddlLogPath()
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
	payload, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = ibos.FileWriteAt(file, payload, 0)
	return err
}

func loadDDLLogEntry() (*ddlLogEntry, error) {
	ddlLogMu.Lock()
	defer ddlLogMu.Unlock()
	path := ddlLogPath()
	exists, err := ibos.FileExists(path)
	if err != nil || !exists {
		return nil, err
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
	var entry ddlLogEntry
	if err := json.Unmarshal(buf, &entry); err != nil {
		return nil, err
	}
	if entry.Op == "" || entry.Table == "" {
		return nil, nil
	}
	return &entry, nil
}

func clearDDLLog() {
	ddlLogMu.Lock()
	defer ddlLogMu.Unlock()
	_ = ibos.FileDelete(ddlLogPath())
}

func recoverDDLLog() ErrCode {
	entry, err := loadDDLLogEntry()
	if err != nil || entry == nil {
		return DB_SUCCESS
	}
	switch entry.Op {
	case ddlOpCreate:
		recoverDDLCreate(entry.Table)
	case ddlOpDrop:
		recoverDDLDrop(entry.Table)
	case ddlOpRename:
		recoverDDLRename(entry.Table, entry.NewTable)
	}
	clearDDLLog()
	return DB_SUCCESS
}

func recoverDDLCreate(name string) {
	if name == "" {
		return
	}
	if dict.DictTableGet(name) != nil {
		return
	}
	dropDDLArtifacts(name)
}

func recoverDDLDrop(name string) {
	if name == "" {
		return
	}
	if table := dict.DictTableGet(name); table != nil {
		_ = dict.DictPersistTableDrop(table)
	}
	dropDDLArtifacts(name)
}

func recoverDDLRename(oldName, newName string) {
	if oldName == "" || newName == "" {
		return
	}
	if table := dict.DictTableGet(oldName); table != nil {
		_ = dict.DictPersistTableRename(table, newName)
	} else if table := dict.DictTableGet(newName); table != nil {
		if oldTable := dict.DictTableGet(oldName); oldTable != nil {
			_ = dict.DictPersistTableDrop(oldTable)
		}
	}
	renameDDLArtifacts(oldName, newName)
}

func dropDDLArtifacts(name string) {
	if name == "" {
		return
	}
	if space := fil.SpaceGetByName(name); space != nil {
		fil.SpaceDrop(space.ID)
	}
	if !filePerTableEnabled() {
		return
	}
	path, err := tableFilePath(name)
	if err != DB_SUCCESS || path == "" {
		return
	}
	_ = ibos.FileDelete(path)
}

func renameDDLArtifacts(oldName, newName string) {
	if oldName == "" || newName == "" {
		return
	}
	if space := fil.SpaceGetByName(oldName); space != nil {
		_ = fil.SpaceRename(space.ID, newName)
	}
	if !filePerTableEnabled() {
		return
	}
	oldPath, errOld := tableFilePath(oldName)
	newPath, errNew := tableFilePath(newName)
	if errOld != DB_SUCCESS || errNew != DB_SUCCESS || oldPath == "" || newPath == "" {
		return
	}
	exists, _ := ibos.FileExists(oldPath)
	if !exists {
		return
	}
	_ = ibos.FileCreateSubdirsIfNeeded(newPath)
	_ = ibos.FileRename(oldPath, newPath)
}
