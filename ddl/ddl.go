package ddl

import (
	"errors"
	"sync"

	"github.com/wilhasse/innodb-go/data"
)

var (
	ErrTableExists   = errors.New("ddl: table already exists")
	ErrTableNotFound = errors.New("ddl: table not found")
	ErrIndexExists   = errors.New("ddl: index already exists")
	ErrIndexNotFound = errors.New("ddl: index not found")
	ErrInvalidName   = errors.New("ddl: invalid name")
)

// Column describes a table column.
type Column struct {
	Name     string
	Type     data.DataType
	Nullable bool
}

// Index describes a table index.
type Index struct {
	Name    string
	Table   string
	Columns []string
	Unique  bool
}

// Table describes a table definition.
type Table struct {
	Name    string
	Columns []Column
	Indexes map[string]*Index
}

// Manager manages in-memory DDL definitions.
type Manager struct {
	mu     sync.Mutex
	tables map[string]*Table
}

// NewManager constructs a DDL manager.
func NewManager() *Manager {
	return &Manager{
		tables: make(map[string]*Table),
	}
}

// CreateTable registers a new table.
func (m *Manager) CreateTable(name string, columns []Column) (*Table, error) {
	if name == "" {
		return nil, ErrInvalidName
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.tables[name]; exists {
		return nil, ErrTableExists
	}
	table := &Table{
		Name:    name,
		Columns: append([]Column(nil), columns...),
		Indexes: make(map[string]*Index),
	}
	m.tables[name] = table
	return table, nil
}

// DropTable removes a table definition.
func (m *Manager) DropTable(name string) error {
	if name == "" {
		return ErrInvalidName
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.tables[name]; !exists {
		return ErrTableNotFound
	}
	delete(m.tables, name)
	return nil
}

// RenameTable renames an existing table.
func (m *Manager) RenameTable(oldName, newName string) error {
	if oldName == "" || newName == "" {
		return ErrInvalidName
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	table, exists := m.tables[oldName]
	if !exists {
		return ErrTableNotFound
	}
	if _, exists := m.tables[newName]; exists {
		return ErrTableExists
	}
	delete(m.tables, oldName)
	table.Name = newName
	for _, idx := range table.Indexes {
		idx.Table = newName
	}
	m.tables[newName] = table
	return nil
}

// CreateIndex registers a new index for a table.
func (m *Manager) CreateIndex(tableName string, index Index) error {
	if tableName == "" || index.Name == "" {
		return ErrInvalidName
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	table, exists := m.tables[tableName]
	if !exists {
		return ErrTableNotFound
	}
	if _, exists := table.Indexes[index.Name]; exists {
		return ErrIndexExists
	}
	idx := index
	idx.Table = tableName
	idx.Columns = append([]string(nil), index.Columns...)
	table.Indexes[index.Name] = &idx
	return nil
}

// DropIndex removes an index from a table.
func (m *Manager) DropIndex(tableName, indexName string) error {
	if tableName == "" || indexName == "" {
		return ErrInvalidName
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	table, exists := m.tables[tableName]
	if !exists {
		return ErrTableNotFound
	}
	if _, exists := table.Indexes[indexName]; !exists {
		return ErrIndexNotFound
	}
	delete(table.Indexes, indexName)
	return nil
}

// GetTable returns a table definition.
func (m *Manager) GetTable(name string) (*Table, bool) {
	if name == "" {
		return nil, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	table, ok := m.tables[name]
	return table, ok
}
