package thr

import (
	stdsync "sync"

	"github.com/wilhasse/innodb-go/os"
	"github.com/wilhasse/innodb-go/ut"
)

type localState struct {
	id     os.ThreadID
	handle os.ThreadHandle
	slotNo ut.Ulint
	inIbuf ut.IBool
}

var (
	localMu  stdsync.Mutex
	localMap map[os.ThreadID]*localState
)

// LocalInit initializes the goroutine-local storage map.
func LocalInit() {
	localMu.Lock()
	defer localMu.Unlock()
	if localMap == nil {
		localMap = make(map[os.ThreadID]*localState)
	}
}

// LocalClose clears goroutine-local storage.
func LocalClose() {
	localMu.Lock()
	localMap = nil
	localMu.Unlock()
}

// LocalCreate registers local storage for the current goroutine.
func LocalCreate() {
	id := os.ThreadGetCurrID()
	localMu.Lock()
	if localMap == nil {
		localMap = make(map[os.ThreadID]*localState)
	}
	if _, ok := localMap[id]; !ok {
		localMap[id] = &localState{
			id:     id,
			handle: os.ThreadHandle{ID: id},
		}
	}
	localMu.Unlock()
}

// LocalFree removes local storage for the given goroutine id.
func LocalFree(id os.ThreadID) {
	localMu.Lock()
	if localMap != nil {
		delete(localMap, id)
	}
	localMu.Unlock()
}

// LocalGetSlotNo returns the slot number for a goroutine.
func LocalGetSlotNo(id os.ThreadID) ut.Ulint {
	local := localGet(id)
	if local == nil {
		return 0
	}
	return local.slotNo
}

// LocalSetSlotNo sets the slot number for a goroutine.
func LocalSetSlotNo(id os.ThreadID, slotNo ut.Ulint) {
	local := localGet(id)
	if local == nil {
		return
	}
	local.slotNo = slotNo
}

// LocalGetInIbufField returns a pointer to the in-ibuf flag for the current goroutine.
func LocalGetInIbufField() *ut.IBool {
	id := os.ThreadGetCurrID()
	local := localGet(id)
	if local == nil {
		return nil
	}
	return &local.inIbuf
}

func localGet(id os.ThreadID) *localState {
	localMu.Lock()
	defer localMu.Unlock()
	if localMap == nil {
		localMap = make(map[os.ThreadID]*localState)
	}
	local := localMap[id]
	if local == nil {
		local = &localState{
			id:     id,
			handle: os.ThreadHandle{ID: id},
		}
		localMap[id] = local
	}
	return local
}
