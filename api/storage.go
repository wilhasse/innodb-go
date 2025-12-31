package api

import (
	"path/filepath"

	"github.com/wilhasse/innodb-go/row"
)

func filePerTableEnabled() bool {
	var enabled Bool
	if err := CfgGet("file_per_table", &enabled); err != DB_SUCCESS {
		return false
	}
	return enabled == IBTrue
}

func dataHomeDir() string {
	var dir string
	if err := CfgGet("data_home_dir", &dir); err != DB_SUCCESS {
		return "."
	}
	if dir == "" {
		return "."
	}
	return dir
}

func tableFilePath(name string) (string, ErrCode) {
	db, table := splitTableName(name)
	if db == "" || table == "" {
		return "", DB_INVALID_INPUT
	}
	path := filepath.Join(dataHomeDir(), db, table+".ibd")
	return path, DB_SUCCESS
}

func attachTableFile(store *row.Store, tableName string) ErrCode {
	if store == nil || !filePerTableEnabled() {
		return DB_SUCCESS
	}
	path, err := tableFilePath(tableName)
	if err != DB_SUCCESS {
		return err
	}
	if err := store.AttachFile(path); err != nil {
		return DB_ERROR
	}
	return DB_SUCCESS
}
