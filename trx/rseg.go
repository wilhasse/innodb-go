package trx

import stdsync "sync"

// RollbackSegment tracks undo logs for transactions.
type RollbackSegment struct {
	ID           uint64
	MaxSize      int
	UpdateUndo   []UndoRecord
	InsertUndo   []UndoRecord
	UpdateCached []UndoRecord
	InsertCached []UndoRecord
	Mu           stdsync.Mutex
}

// RsegSystem tracks rollback segments in memory.
type RsegSystem struct {
	Mu       stdsync.Mutex
	Segments map[uint64]*RollbackSegment
	Order    []uint64
}

// RsegSys is the global rollback segment registry.
var RsegSys = &RsegSystem{Segments: make(map[uint64]*RollbackSegment)}

// RsegVarInit resets the rollback segment system.
func RsegVarInit() {
	RsegSys = &RsegSystem{Segments: make(map[uint64]*RollbackSegment)}
}

// RsegGetOnID returns the rollback segment for a given id.
func RsegGetOnID(id uint64) *RollbackSegment {
	if RsegSys == nil {
		return nil
	}
	RsegSys.Mu.Lock()
	rseg := RsegSys.Segments[id]
	RsegSys.Mu.Unlock()
	return rseg
}

// RsegCreate registers a rollback segment.
func RsegCreate(id uint64, maxSize int) *RollbackSegment {
	if RsegSys == nil {
		RsegSys = &RsegSystem{Segments: make(map[uint64]*RollbackSegment)}
	}
	RsegSys.Mu.Lock()
	defer RsegSys.Mu.Unlock()
	if existing := RsegSys.Segments[id]; existing != nil {
		return existing
	}
	rseg := &RollbackSegment{ID: id, MaxSize: maxSize}
	RsegSys.Segments[id] = rseg
	RsegSys.Order = append(RsegSys.Order, id)
	return rseg
}

// RsegFree removes a rollback segment from the registry.
func RsegFree(rseg *RollbackSegment) {
	if rseg == nil || RsegSys == nil {
		return
	}
	RsegSys.Mu.Lock()
	delete(RsegSys.Segments, rseg.ID)
	for i, id := range RsegSys.Order {
		if id == rseg.ID {
			RsegSys.Order = append(RsegSys.Order[:i], RsegSys.Order[i+1:]...)
			break
		}
	}
	RsegSys.Mu.Unlock()
}

// AddUpdateUndo appends an update undo record.
func (rseg *RollbackSegment) AddUpdateUndo(rec UndoRecord) bool {
	if rseg == nil {
		return false
	}
	rseg.Mu.Lock()
	defer rseg.Mu.Unlock()
	if rseg.maxedLocked() {
		return false
	}
	rseg.UpdateUndo = append(rseg.UpdateUndo, rec)
	return true
}

// AddInsertUndo appends an insert undo record.
func (rseg *RollbackSegment) AddInsertUndo(rec UndoRecord) bool {
	if rseg == nil {
		return false
	}
	rseg.Mu.Lock()
	defer rseg.Mu.Unlock()
	if rseg.maxedLocked() {
		return false
	}
	rseg.InsertUndo = append(rseg.InsertUndo, rec)
	return true
}

// CacheUpdateUndo caches an update undo record for reuse.
func (rseg *RollbackSegment) CacheUpdateUndo(rec UndoRecord) {
	if rseg == nil {
		return
	}
	rseg.Mu.Lock()
	rseg.UpdateCached = append(rseg.UpdateCached, rec)
	rseg.Mu.Unlock()
}

// CacheInsertUndo caches an insert undo record for reuse.
func (rseg *RollbackSegment) CacheInsertUndo(rec UndoRecord) {
	if rseg == nil {
		return
	}
	rseg.Mu.Lock()
	rseg.InsertCached = append(rseg.InsertCached, rec)
	rseg.Mu.Unlock()
}

// PopCachedUpdateUndo returns a cached update undo record.
func (rseg *RollbackSegment) PopCachedUpdateUndo() (UndoRecord, bool) {
	if rseg == nil {
		return UndoRecord{}, false
	}
	rseg.Mu.Lock()
	defer rseg.Mu.Unlock()
	if len(rseg.UpdateCached) == 0 {
		return UndoRecord{}, false
	}
	idx := len(rseg.UpdateCached) - 1
	rec := rseg.UpdateCached[idx]
	rseg.UpdateCached = rseg.UpdateCached[:idx]
	return rec, true
}

// PopCachedInsertUndo returns a cached insert undo record.
func (rseg *RollbackSegment) PopCachedInsertUndo() (UndoRecord, bool) {
	if rseg == nil {
		return UndoRecord{}, false
	}
	rseg.Mu.Lock()
	defer rseg.Mu.Unlock()
	if len(rseg.InsertCached) == 0 {
		return UndoRecord{}, false
	}
	idx := len(rseg.InsertCached) - 1
	rec := rseg.InsertCached[idx]
	rseg.InsertCached = rseg.InsertCached[:idx]
	return rec, true
}

func (rseg *RollbackSegment) maxedLocked() bool {
	if rseg.MaxSize <= 0 {
		return false
	}
	return len(rseg.UpdateUndo)+len(rseg.InsertUndo) >= rseg.MaxSize
}
