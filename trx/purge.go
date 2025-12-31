package trx

import (
	stdsync "sync"

	"github.com/wilhasse/innodb-go/read"
	"github.com/wilhasse/innodb-go/ut"
)

// PurgeState mirrors purge system state.
type PurgeState int

const (
	// PurgeOn indicates purge is running.
	PurgeOn PurgeState = 1
	// PurgeStop indicates purge should stop.
	PurgeStop PurgeState = 2
)

// PurgeRecord represents a purgeable undo record.
type PurgeRecord struct {
	TrxID  uint64
	UndoNo uint64
}

// PurgeInfo tracks an in-flight purge record.
type PurgeInfo struct {
	TrxID  uint64
	UndoNo uint64
	InUse  bool
}

// PurgeArray stores in-flight purge records.
type PurgeArray struct {
	Infos []*PurgeInfo
	Used  int
}

// NewPurgeArray allocates a purge array.
func NewPurgeArray(size int) *PurgeArray {
	if size < 0 {
		size = 0
	}
	return &PurgeArray{Infos: make([]*PurgeInfo, size)}
}

// Store reserves a slot for an in-flight purge record.
func (arr *PurgeArray) Store(trxID, undoNo uint64) *PurgeInfo {
	if arr == nil {
		return nil
	}
	for i, info := range arr.Infos {
		if info == nil {
			info = &PurgeInfo{}
			arr.Infos[i] = info
		}
		if !info.InUse {
			info.TrxID = trxID
			info.UndoNo = undoNo
			info.InUse = true
			arr.Used++
			return info
		}
	}
	info := &PurgeInfo{TrxID: trxID, UndoNo: undoNo, InUse: true}
	arr.Infos = append(arr.Infos, info)
	arr.Used++
	return info
}

// Remove releases a purge record slot.
func (arr *PurgeArray) Remove(info *PurgeInfo) {
	if arr == nil || info == nil || !info.InUse {
		return
	}
	info.InUse = false
	if arr.Used > 0 {
		arr.Used--
	}
}

// Biggest returns the largest transaction/undo pair in use.
func (arr *PurgeArray) Biggest() (uint64, uint64, bool) {
	if arr == nil || arr.Used == 0 {
		return 0, 0, false
	}
	var maxTrx uint64
	var maxUndo uint64
	found := false
	for _, info := range arr.Infos {
		if info == nil || !info.InUse {
			continue
		}
		if !found || info.TrxID > maxTrx || (info.TrxID == maxTrx && info.UndoNo >= maxUndo) {
			maxTrx = info.TrxID
			maxUndo = info.UndoNo
			found = true
		}
	}
	if !found {
		return 0, 0, false
	}
	return maxTrx, maxUndo, true
}

// PurgeSystem coordinates purge operations.
type PurgeSystem struct {
	Mu           stdsync.Mutex
	State        PurgeState
	View         *read.ReadView
	Arr          *PurgeArray
	Queue        []PurgeRecord
	PagesHandled ut.Ulint
	HandleLimit  ut.Ulint
	PurgeTrxNo   uint64
	PurgeUndoNo  uint64
	NextStored   bool
}

// PurgeSys is the global purge coordinator.
var PurgeSys *PurgeSystem

// PurgeDummyRecord is returned when a log can be skipped.
var PurgeDummyRecord PurgeRecord

// PurgeVarInit resets purge globals.
func PurgeVarInit() {
	PurgeSys = nil
	PurgeDummyRecord = PurgeRecord{}
}

// PurgeSysCreate initializes the global purge system.
func PurgeSysCreate() {
	if PurgeSys != nil {
		return
	}
	PurgeSys = &PurgeSystem{
		State: PurgeOn,
		Arr:   NewPurgeArray(16),
	}
}

// PurgeSysClose releases the purge system.
func PurgeSysClose() {
	PurgeSys = nil
}

// PurgeUpdateUndoMustExist reports whether undo data must still exist.
func PurgeUpdateUndoMustExist(trxID uint64) bool {
	if PurgeSys == nil {
		return false
	}
	PurgeSys.Mu.Lock()
	view := PurgeSys.View
	PurgeSys.Mu.Unlock()
	if view == nil {
		return false
	}
	return !view.Sees(trxID)
}

// PurgeAddUpdateUndoToHistory enqueues a purge record.
func PurgeAddUpdateUndoToHistory(trxID, undoNo uint64) {
	if PurgeSys == nil {
		return
	}
	PurgeSys.Mu.Lock()
	PurgeSys.Queue = append(PurgeSys.Queue, PurgeRecord{TrxID: trxID, UndoNo: undoNo})
	PurgeSys.Mu.Unlock()
}

// PurgeFetchNextRec returns the next record to purge and its tracking slot.
func PurgeFetchNextRec() (*PurgeRecord, *PurgeInfo) {
	if PurgeSys == nil {
		return nil, nil
	}
	PurgeSys.Mu.Lock()
	defer PurgeSys.Mu.Unlock()
	if len(PurgeSys.Queue) == 0 {
		return nil, nil
	}
	rec := PurgeSys.Queue[0]
	PurgeSys.Queue = PurgeSys.Queue[1:]
	if PurgeSys.Arr == nil {
		PurgeSys.Arr = NewPurgeArray(16)
	}
	info := PurgeSys.Arr.Store(rec.TrxID, rec.UndoNo)
	return &rec, info
}

// PurgeRecRelease releases a reserved purge record.
func PurgeRecRelease(info *PurgeInfo) {
	if PurgeSys == nil || info == nil {
		return
	}
	PurgeSys.Mu.Lock()
	if PurgeSys.Arr != nil {
		PurgeSys.Arr.Remove(info)
	}
	PurgeSys.Mu.Unlock()
}

// PurgeRun runs a purge batch and returns the number of records handled.
func PurgeRun() ut.Ulint {
	if PurgeSys == nil {
		return 0
	}
	handled := ut.Ulint(0)
	for {
		PurgeSys.Mu.Lock()
		if len(PurgeSys.Queue) == 0 {
			PurgeSys.Mu.Unlock()
			break
		}
		if PurgeSys.HandleLimit > 0 && handled >= PurgeSys.HandleLimit {
			PurgeSys.Mu.Unlock()
			break
		}
		rec := PurgeSys.Queue[0]
		PurgeSys.Queue = PurgeSys.Queue[1:]
		if PurgeSys.Arr == nil {
			PurgeSys.Arr = NewPurgeArray(16)
		}
		info := PurgeSys.Arr.Store(rec.TrxID, rec.UndoNo)
		PurgeSys.PagesHandled++
		PurgeSys.Arr.Remove(info)
		PurgeSys.Mu.Unlock()
		handled++
	}
	return handled
}

// PurgeSysPrint is a placeholder for purge system diagnostics.
func PurgeSysPrint() {
}
