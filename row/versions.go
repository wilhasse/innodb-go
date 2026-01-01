package row

import (
	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/read"
)

// RecordVersion tracks a tuple version for the given key.
func (store *Store) RecordVersion(key []byte, trxID uint64, tuple *data.Tuple) {
	if store == nil || len(key) == 0 {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.ensureIndex()
	if store.versions == nil {
		store.versions = make(map[string]*VersionedRow)
	}
	k := string(key)
	vr := store.versions[k]
	if vr == nil {
		store.versions[k] = NewVersionedRow(trxID, tuple)
		return
	}
	vr.AddVersion(trxID, tuple)
}

// VersionForView returns the tuple visible to a read view for the key.
func (store *Store) VersionForView(key []byte, view *read.ReadView) (*data.Tuple, bool) {
	if store == nil || len(key) == 0 {
		return nil, false
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.versions == nil {
		return nil, false
	}
	vr := store.versions[string(key)]
	if vr == nil {
		return nil, false
	}
	return vr.VersionForView(view), true
}
