package dict

import (
	"sort"
	"strings"
)

// DictGetFirstTableNameInDB returns the first table name in a database.
func DictGetFirstTableNameInDB(db string) string {
	if DictSys == nil {
		return ""
	}
	if db == "" {
		return ""
	}
	if !strings.HasSuffix(db, "/") {
		db += "/"
	}
	DictSys.mu.Lock()
	defer DictSys.mu.Unlock()
	names := make([]string, 0)
	for name := range DictSys.Tables {
		if strings.HasPrefix(name, db) {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	return names[0]
}

// DictListTables returns all table names.
func DictListTables() []string {
	if DictSys == nil {
		return nil
	}
	DictSys.mu.Lock()
	defer DictSys.mu.Unlock()
	names := make([]string, 0, len(DictSys.Tables))
	for name := range DictSys.Tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// DictLoadSysTable ensures a system table is cached.
func DictLoadSysTable(table *Table) error {
	if table == nil {
		return ErrTableNotFound
	}
	if DictSys == nil {
		DictInitCore()
	}
	DictSys.mu.Lock()
	defer DictSys.mu.Unlock()
	if _, ok := DictSys.Tables[table.Name]; !ok {
		DictSys.Tables[table.Name] = table
	}
	return nil
}

// DictLoadTable returns a table by name.
func DictLoadTable(name string) (*Table, error) {
	if DictSys == nil {
		return nil, ErrTableNotFound
	}
	DictSys.mu.Lock()
	defer DictSys.mu.Unlock()
	table, ok := DictSys.Tables[name]
	if !ok {
		return nil, ErrTableNotFound
	}
	return table, nil
}
