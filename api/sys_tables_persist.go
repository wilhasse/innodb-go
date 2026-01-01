package api

import (
	"errors"
	"strings"

	"github.com/wilhasse/innodb-go/btr"
	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/dict"
	"github.com/wilhasse/innodb-go/row"
)

type sysTablePersister struct{}

type sysTableStores struct {
	tables  *row.Store
	columns *row.Store
	indexes *row.Store
	fields  *row.Store
}

func (p *sysTablePersister) LoadSysRows() (dict.SysRows, error) {
	stores, err := buildSysTableStores()
	if err != nil {
		return dict.SysRows{}, err
	}
	if err := loadSysStores(stores); err != nil {
		return dict.SysRows{}, err
	}
	return dict.SysRows{
		Tables:  cloneSysRows(stores.tables),
		Columns: cloneSysRows(stores.columns),
		Indexes: cloneSysRows(stores.indexes),
		Fields:  cloneSysRows(stores.fields),
	}, nil
}

func (p *sysTablePersister) PersistSysRows(rows dict.SysRows) error {
	stores, err := buildSysTableStores()
	if err != nil {
		return err
	}
	if err := loadSysStores(stores); err != nil {
		return err
	}
	if err := replaceStoreRows(stores.tables, rows.Tables); err != nil {
		return err
	}
	if err := replaceStoreRows(stores.columns, rows.Columns); err != nil {
		return err
	}
	if err := replaceStoreRows(stores.indexes, rows.Indexes); err != nil {
		return err
	}
	if err := replaceStoreRows(stores.fields, rows.Fields); err != nil {
		return err
	}
	if dict.DictSys != nil {
		dict.DictSys.Header.TablesRoot = stores.tables.PageTree.RootPage
		dict.DictSys.Header.ColumnsRoot = stores.columns.PageTree.RootPage
		dict.DictSys.Header.IndexesRoot = stores.indexes.PageTree.RootPage
		dict.DictSys.Header.FieldsRoot = stores.fields.PageTree.RootPage
	}
	return nil
}

func buildSysTableStores() (*sysTableStores, error) {
	if dict.DictSys == nil {
		return nil, errors.New("api: dict not initialized")
	}
	header := dict.DictSys.Header
	tables, err := makeSysTableStore(dict.DictSys.SysTables, header.TablesRoot)
	if err != nil {
		return nil, err
	}
	columns, err := makeSysTableStore(dict.DictSys.SysColumns, header.ColumnsRoot)
	if err != nil {
		return nil, err
	}
	indexes, err := makeSysTableStore(dict.DictSys.SysIndexes, header.IndexesRoot)
	if err != nil {
		return nil, err
	}
	fields, err := makeSysTableStore(dict.DictSys.SysFields, header.FieldsRoot)
	if err != nil {
		return nil, err
	}
	return &sysTableStores{
		tables:  tables,
		columns: columns,
		indexes: indexes,
		fields:  fields,
	}, nil
}

func makeSysTableStore(table *dict.Table, rootPage uint32) (*row.Store, error) {
	if table == nil {
		return nil, errors.New("api: nil sys table")
	}
	clustered := clusteredIndex(table)
	if clustered == nil {
		return nil, errors.New("api: missing clustered index")
	}
	keyFields, err := columnPositions(table, clustered.Fields)
	if err != nil {
		return nil, err
	}
	store := row.NewStore(-1)
	store.PrimaryKeyFields = keyFields
	store.PrimaryKeyPrefixes = make([]int, len(keyFields))
	store.SpaceID = dict.DictHdrSpace
	store.PageTree = btr.NewPageTree(dict.DictHdrSpace, row.CompareKeys)
	store.PageTree.RootPage = rootPage
	return store, nil
}

func clusteredIndex(table *dict.Table) *dict.Index {
	if table == nil {
		return nil
	}
	for _, idx := range table.Indexes {
		if idx != nil && idx.Clustered {
			return idx
		}
	}
	return nil
}

func columnPositions(table *dict.Table, fields []string) ([]int, error) {
	if table == nil {
		return nil, errors.New("api: nil sys table")
	}
	positions := make([]int, 0, len(fields))
	for _, name := range fields {
		pos := -1
		for i, col := range table.Columns {
			if strings.EqualFold(col.Name, name) {
				pos = i
				break
			}
		}
		if pos < 0 {
			return nil, errors.New("api: missing sys column")
		}
		positions = append(positions, pos)
	}
	return positions, nil
}

func loadSysStores(stores *sysTableStores) error {
	if stores == nil {
		return errors.New("api: nil sys stores")
	}
	if err := stores.tables.LoadFromPages(); err != nil {
		return err
	}
	if err := stores.columns.LoadFromPages(); err != nil {
		return err
	}
	if err := stores.indexes.LoadFromPages(); err != nil {
		return err
	}
	if err := stores.fields.LoadFromPages(); err != nil {
		return err
	}
	return nil
}

func replaceStoreRows(store *row.Store, rows []*data.Tuple) error {
	if store == nil {
		return errors.New("api: nil store")
	}
	existing := append([]*data.Tuple(nil), store.Rows...)
	for _, row := range existing {
		if row == nil {
			continue
		}
		store.RemoveTuple(row)
	}
	for _, row := range rows {
		if row == nil {
			continue
		}
		if err := store.Insert(row); err != nil {
			return err
		}
	}
	return nil
}

func cloneSysRows(store *row.Store) []*data.Tuple {
	if store == nil || len(store.Rows) == 0 {
		return nil
	}
	rows := make([]*data.Tuple, 0, len(store.Rows))
	for _, row := range store.Rows {
		if row != nil {
			rows = append(rows, row)
		}
	}
	return rows
}
