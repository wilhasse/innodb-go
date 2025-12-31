package dict

import (
	"sync"

	"github.com/wilhasse/innodb-go/ut"
)

// Header stores the dictionary header counters and roots.
type Header struct {
	RowID        ut.Dulint
	TableID      ut.Dulint
	IndexID      ut.Dulint
	MixID        ut.Dulint
	TablesRoot   uint32
	TableIDsRoot uint32
	ColumnsRoot  uint32
	IndexesRoot  uint32
	FieldsRoot   uint32
}

// System holds the dictionary cache and header state.
type System struct {
	mu sync.Mutex

	Header Header
	RowID  ut.Dulint

	Tables map[string]*Table

	SysTables  *Table
	SysColumns *Table
	SysIndexes *Table
	SysFields  *Table
	SysRows    SysRows
}

// DictSys is the global dictionary system.
var DictSys *System
