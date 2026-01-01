package dict

import (
	"strings"

	"github.com/wilhasse/innodb-go/ut"
)

// DictInitCore initializes dictionary globals.
func DictInitCore() {
	DictSys = &System{
		Tables: make(map[string]*Table),
	}
}

// DictClose resets dictionary globals.
func DictClose() {
	DictSys = nil
	dictDataDir = ""
}

// DictCasednStr lowercases a UTF-8 string.
func DictCasednStr(s string) string {
	return strings.ToLower(s)
}

// DictGetDBNameLen returns the length of the db name prefix.
func DictGetDBNameLen(name string) int {
	if name == "" {
		return 0
	}
	if idx := strings.IndexByte(name, '/'); idx >= 0 {
		return idx
	}
	return len(name)
}

// DictRemoveDBName removes the db name prefix from a table name.
func DictRemoveDBName(name string) string {
	if name == "" {
		return ""
	}
	if idx := strings.IndexByte(name, '/'); idx >= 0 && idx+1 < len(name) {
		return name[idx+1:]
	}
	return name
}

// DictTableGetOnID looks up a table by id.
func DictTableGetOnID(id ut.Dulint) *Table {
	if DictSys == nil {
		return nil
	}
	DictSys.mu.Lock()
	defer DictSys.mu.Unlock()
	for _, table := range DictSys.Tables {
		if dulintToUint64(table.ID) == dulintToUint64(id) {
			return table
		}
	}
	return nil
}

// DictTableGet returns a table by name.
func DictTableGet(name string) *Table {
	if DictSys == nil || name == "" {
		return nil
	}
	DictSys.mu.Lock()
	defer DictSys.mu.Unlock()
	return DictSys.Tables[name]
}

// DictTableAddToCache adds a table to the dictionary cache.
func DictTableAddToCache(table *Table) error {
	if table == nil || table.Name == "" {
		return ErrInvalidName
	}
	if DictSys == nil {
		DictInitCore()
	}
	DictSys.mu.Lock()
	defer DictSys.mu.Unlock()
	if _, exists := DictSys.Tables[table.Name]; exists {
		return ErrTableExists
	}
	DictSys.Tables[table.Name] = table
	return nil
}

// DictTableRemoveFromCache removes a table from the dictionary cache.
func DictTableRemoveFromCache(table *Table) {
	if DictSys == nil || table == nil {
		return
	}
	DictSys.mu.Lock()
	defer DictSys.mu.Unlock()
	delete(DictSys.Tables, table.Name)
}

// DictTableRenameInCache renames a table in the cache.
func DictTableRenameInCache(table *Table, newName string) error {
	if DictSys == nil || table == nil {
		return ErrTableNotFound
	}
	if newName == "" {
		return ErrInvalidName
	}
	DictSys.mu.Lock()
	defer DictSys.mu.Unlock()
	if _, exists := DictSys.Tables[newName]; exists {
		return ErrTableExists
	}
	delete(DictSys.Tables, table.Name)
	table.Name = newName
	DictSys.Tables[newName] = table
	return nil
}

// DictIndexAddToCache registers an index with a table.
func DictIndexAddToCache(table *Table, index *Index) error {
	if table == nil || index == nil || index.Name == "" {
		return ErrInvalidName
	}
	if table.Indexes == nil {
		table.Indexes = make(map[string]*Index)
	}
	if _, exists := table.Indexes[index.Name]; exists {
		return ErrIndexExists
	}
	table.Indexes[index.Name] = index
	return nil
}

// DictIndexRemoveFromCache removes an index from a table.
func DictIndexRemoveFromCache(table *Table, index *Index) {
	if table == nil || index == nil {
		return
	}
	delete(table.Indexes, index.Name)
}
