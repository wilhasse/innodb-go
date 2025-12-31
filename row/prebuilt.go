package row

import (
	"errors"

	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/dict"
	"github.com/wilhasse/innodb-go/trx"
)

// Prebuilt lifecycle markers.
const (
	PrebuiltAllocated uint32 = 0xfeedbeef
	PrebuiltFreed     uint32 = 0xdeadc0de
)

// Select lock types.
const (
	SelectLockNone = iota
)

// RowCache stores cached rows for a prebuilt handle.
type RowCache struct {
	Max  int
	Rows []*data.Tuple
}

// Add inserts a row into the cache, evicting oldest when full.
func (cache *RowCache) Add(row *data.Tuple) {
	if cache == nil || row == nil || cache.Max <= 0 {
		return
	}
	if len(cache.Rows) >= cache.Max {
		cache.Rows = cache.Rows[1:]
	}
	cache.Rows = append(cache.Rows, row)
}

// Prebuilt holds per-table state for row operations.
type Prebuilt struct {
	Magic          uint32
	Magic2         uint32
	Table          *dict.Table
	Trx            *trx.Trx
	SQLStatStart   bool
	SimpleSelect   bool
	SelectLockType int
	RowCache       RowCache
}

// NewPrebuilt allocates a prebuilt struct for a table.
func NewPrebuilt(table *dict.Table, cacheSize int) *Prebuilt {
	return &Prebuilt{
		Magic:        PrebuiltAllocated,
		Magic2:       PrebuiltAllocated,
		Table:        table,
		SQLStatStart: true,
		RowCache:     RowCache{Max: cacheSize},
		SelectLockType: SelectLockNone,
	}
}

// Reset clears transient state in the prebuilt struct.
func (p *Prebuilt) Reset() error {
	if p == nil || p.Magic != PrebuiltAllocated || p.Magic2 != PrebuiltAllocated {
		return errors.New("row: invalid prebuilt")
	}
	p.SQLStatStart = true
	p.SimpleSelect = false
	p.SelectLockType = SelectLockNone
	p.Trx = nil
	p.RowCache.Rows = nil
	return nil
}

// UpdateTrx updates the transaction pointer.
func (p *Prebuilt) UpdateTrx(trx *trx.Trx) error {
	if p == nil || p.Magic != PrebuiltAllocated || p.Magic2 != PrebuiltAllocated {
		return errors.New("row: invalid prebuilt")
	}
	p.Trx = trx
	return nil
}

// Free marks the prebuilt struct as freed and clears references.
func (p *Prebuilt) Free() error {
	if p == nil {
		return nil
	}
	if p.Magic != PrebuiltAllocated || p.Magic2 != PrebuiltAllocated {
		return errors.New("row: invalid prebuilt")
	}
	p.Magic = PrebuiltFreed
	p.Magic2 = PrebuiltFreed
	p.Table = nil
	p.Trx = nil
	p.RowCache.Rows = nil
	return nil
}
