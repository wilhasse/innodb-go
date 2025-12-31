package api

import (
	"sync"

	"github.com/wilhasse/innodb-go/data"
)

var (
	tupleMetaMu sync.Mutex
	tupleMeta   = map[*data.Tuple][]ColMeta{}
)

func registerTupleMeta(tpl *data.Tuple, schema *TableSchema) {
	if tpl == nil || schema == nil {
		return
	}
	meta := make([]ColMeta, len(schema.Columns))
	for i, col := range schema.Columns {
		meta[i] = ColMeta{
			Type:    col.Type,
			Attr:    col.Attr,
			TypeLen: col.Size,
		}
	}
	tupleMetaMu.Lock()
	tupleMeta[tpl] = meta
	tupleMetaMu.Unlock()
}

func unregisterTupleMeta(tpl *data.Tuple) {
	if tpl == nil {
		return
	}
	tupleMetaMu.Lock()
	delete(tupleMeta, tpl)
	tupleMetaMu.Unlock()
}

// ColGetMeta populates col metadata and returns the column length.
func ColGetMeta(tpl *data.Tuple, col int, out *ColMeta) Ulint {
	if tpl == nil || out == nil || col < 0 {
		return Ulint(IBSQLNull)
	}
	tupleMetaMu.Lock()
	meta := tupleMeta[tpl]
	tupleMetaMu.Unlock()
	if meta != nil && col < len(meta) {
		*out = meta[col]
	} else {
		*out = ColMeta{}
	}
	return ColGetLen(tpl, col)
}
