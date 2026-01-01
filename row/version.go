package row

import (
	"github.com/wilhasse/innodb-go/data"
	"github.com/wilhasse/innodb-go/read"
)

// RowVersion stores a tuple version tagged with a transaction id.
type RowVersion struct {
	TrxID uint64
	Tuple *data.Tuple
}

// VersionedRow tracks versions of a row over time.
type VersionedRow struct {
	Versions []RowVersion
}

// NewVersionedRow creates a versioned row with an initial version.
func NewVersionedRow(trxID uint64, tuple *data.Tuple) *VersionedRow {
	vr := &VersionedRow{}
	if tuple != nil || trxID != 0 {
		vr.Versions = append(vr.Versions, RowVersion{TrxID: trxID, Tuple: tuple})
	}
	return vr
}

// AddVersion appends a new row version.
func (vr *VersionedRow) AddVersion(trxID uint64, tuple *data.Tuple) {
	if vr == nil {
		return
	}
	if tuple == nil && trxID == 0 {
		return
	}
	vr.Versions = append(vr.Versions, RowVersion{TrxID: trxID, Tuple: tuple})
}

// Current returns the latest version.
func (vr *VersionedRow) Current() *data.Tuple {
	if vr == nil || len(vr.Versions) == 0 {
		return nil
	}
	return vr.Versions[len(vr.Versions)-1].Tuple
}

// VersionFor returns the latest version visible for the trx id.
func (vr *VersionedRow) VersionFor(trxID uint64) *data.Tuple {
	if vr == nil {
		return nil
	}
	var best *data.Tuple
	var bestID uint64
	for _, v := range vr.Versions {
		if v.TrxID <= trxID && v.TrxID >= bestID {
			best = v.Tuple
			bestID = v.TrxID
		}
	}
	return best
}

// VersionForView returns the latest version visible to a read view.
func (vr *VersionedRow) VersionForView(view *read.ReadView) *data.Tuple {
	if vr == nil {
		return nil
	}
	if view == nil {
		return vr.Current()
	}
	for i := len(vr.Versions) - 1; i >= 0; i-- {
		v := vr.Versions[i]
		if view.Sees(v.TrxID) {
			return v.Tuple
		}
	}
	return nil
}
