package trx

import (
	stdsync "sync"

	"github.com/wilhasse/innodb-go/read"
)

// TrxSystem holds global transaction system state.
type TrxSystem struct {
	Mu          stdsync.Mutex
	Initialized bool
	NextID      uint64
	Active      []*Trx
	Rsegs       []*RollbackSegment
	ReadViews   *read.ViewList
}

// DoublewriteBuffer tracks the doublewrite buffer ranges.
type DoublewriteBuffer struct {
	Block1    uint64
	Block2    uint64
	BlockSize uint64
}

// TrxSys is the global transaction system.
var TrxSys *TrxSystem

// TrxDoublewrite is the global doublewrite buffer descriptor.
var TrxDoublewrite *DoublewriteBuffer

// TrxSysVarInit resets global transaction system state.
func TrxSysVarInit() {
	TrxSys = nil
	TrxDoublewrite = nil
}

// TrxSysInit initializes the transaction system.
func TrxSysInit() {
	if TrxSys != nil {
		return
	}
	TrxSys = &TrxSystem{
		Initialized: true,
		NextID:      1,
		ReadViews:   &read.ViewList{},
	}
}

// TrxSysClose tears down the transaction system.
func TrxSysClose() {
	TrxSys = nil
	TrxDoublewrite = nil
}

// TrxSysAllocID reserves a new transaction id.
func TrxSysAllocID() uint64 {
	if TrxSys == nil {
		return 0
	}
	TrxSys.Mu.Lock()
	id := TrxSys.NextID
	TrxSys.NextID++
	TrxSys.Mu.Unlock()
	return id
}

// TrxSysAddActive registers an active transaction.
func TrxSysAddActive(trx *Trx) {
	if TrxSys == nil || trx == nil {
		return
	}
	TrxSys.Mu.Lock()
	TrxSys.Active = append(TrxSys.Active, trx)
	TrxSys.Mu.Unlock()
}

// TrxSysRemoveActive unregisters an active transaction.
func TrxSysRemoveActive(trx *Trx) {
	if TrxSys == nil || trx == nil {
		return
	}
	TrxSys.Mu.Lock()
	for i, t := range TrxSys.Active {
		if t == trx {
			TrxSys.Active = append(TrxSys.Active[:i], TrxSys.Active[i+1:]...)
			break
		}
	}
	TrxSys.Mu.Unlock()
}

// TrxDoublewriteInit initializes the doublewrite buffer descriptor.
func TrxDoublewriteInit(block1, block2, blockSize uint64) {
	TrxDoublewrite = &DoublewriteBuffer{
		Block1:    block1,
		Block2:    block2,
		BlockSize: blockSize,
	}
}

// TrxDoublewritePageInside reports whether pageNo is inside the buffer.
func TrxDoublewritePageInside(pageNo uint64) bool {
	if TrxDoublewrite == nil || TrxDoublewrite.BlockSize == 0 {
		return false
	}
	if pageNo >= TrxDoublewrite.Block1 && pageNo < TrxDoublewrite.Block1+TrxDoublewrite.BlockSize {
		return true
	}
	if pageNo >= TrxDoublewrite.Block2 && pageNo < TrxDoublewrite.Block2+TrxDoublewrite.BlockSize {
		return true
	}
	return false
}
