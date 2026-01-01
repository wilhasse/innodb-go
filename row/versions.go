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

// RollbackVersions removes versions created by a transaction for the key.
func (store *Store) RollbackVersions(key []byte, trxID uint64) {
	if store == nil || len(key) == 0 || trxID == 0 {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.versions == nil {
		return
	}
	k := string(key)
	vr := store.versions[k]
	if vr == nil || len(vr.Versions) == 0 {
		return
	}
	dst := vr.Versions[:0]
	for _, v := range vr.Versions {
		if v.TrxID != trxID {
			dst = append(dst, v)
		}
	}
	if len(dst) == 0 {
		delete(store.versions, k)
		return
	}
	vr.Versions = dst
}

// PurgeVersions removes versions older than a minimum trx id.
func (store *Store) PurgeVersions(minTrxID uint64, purgeAll bool) int {
	if store == nil {
		return 0
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.versions == nil {
		return 0
	}
	removed := 0
	for key, vr := range store.versions {
		if vr == nil || len(vr.Versions) == 0 {
			delete(store.versions, key)
			continue
		}
		before := len(vr.Versions)
		if purgeAll {
			latest := vr.Versions[len(vr.Versions)-1]
			if latest.Tuple == nil {
				delete(store.versions, key)
				removed += before
				continue
			}
			vr.Versions = vr.Versions[len(vr.Versions)-1:]
			removed += before - 1
			continue
		}
		lastIdx := len(vr.Versions) - 1
		dst := vr.Versions[:0]
		for i, v := range vr.Versions {
			if i == lastIdx || v.TrxID >= minTrxID {
				dst = append(dst, v)
			}
		}
		if len(dst) == 0 {
			delete(store.versions, key)
		} else {
			vr.Versions = dst
		}
		removed += before - len(dst)
	}
	return removed
}
