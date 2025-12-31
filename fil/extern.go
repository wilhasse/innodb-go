package fil

import "sync"

var (
	externMu    sync.Mutex
	externNext  uint64
	externStore = map[uint64][]byte{}
)

// ExternStore writes data to the external store and returns its id.
func ExternStore(data []byte) uint64 {
	externMu.Lock()
	defer externMu.Unlock()
	externNext++
	id := externNext
	clone := make([]byte, len(data))
	copy(clone, data)
	externStore[id] = clone
	return id
}

// ExternGet returns the stored data for an external id.
func ExternGet(id uint64) []byte {
	externMu.Lock()
	defer externMu.Unlock()
	data := externStore[id]
	if data == nil {
		return nil
	}
	clone := make([]byte, len(data))
	copy(clone, data)
	return clone
}

// ExternFree removes external data by id.
func ExternFree(id uint64) {
	externMu.Lock()
	defer externMu.Unlock()
	delete(externStore, id)
}

func externReset() {
	externMu.Lock()
	defer externMu.Unlock()
	externNext = 0
	externStore = map[uint64][]byte{}
}
