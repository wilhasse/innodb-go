package api

import (
	"github.com/wilhasse/innodb-go/read"
	"github.com/wilhasse/innodb-go/trx"
	"github.com/wilhasse/innodb-go/ut"
)

func purgeIfNeeded() {
	if trx.TrxSys == nil {
		updatePurgeView(nil)
		return
	}
	views, viewCount, activeCount := snapshotReadViews()
	updatePurgeView(views)
	if viewCount > 0 || activeCount > 0 {
		return
	}
	removed := purgeVersions(0, true)
	if removed == 0 || trx.PurgeSys == nil {
		return
	}
	trx.PurgeSys.Mu.Lock()
	trx.PurgeSys.PagesHandled += ut.Ulint(removed)
	trx.PurgeSys.Mu.Unlock()
}

func snapshotReadViews() ([]*read.ReadView, int, int) {
	if trx.TrxSys == nil {
		return nil, 0, 0
	}
	trx.TrxSys.Mu.Lock()
	defer trx.TrxSys.Mu.Unlock()
	views := []*read.ReadView(nil)
	if trx.TrxSys.ReadViews != nil && len(trx.TrxSys.ReadViews.Views) > 0 {
		views = append(views, trx.TrxSys.ReadViews.Views...)
	}
	activeCount := len(trx.TrxSys.Active)
	return views, len(views), activeCount
}

func purgeVersions(minTrxID uint64, purgeAll bool) int {
	removed := 0
	schemaMu.Lock()
	defer schemaMu.Unlock()
	for _, db := range databases {
		for _, table := range db.Tables {
			if table == nil || table.Store == nil {
				continue
			}
			removed += table.Store.PurgeVersions(minTrxID, purgeAll)
		}
	}
	return removed
}

func updatePurgeView(views []*read.ReadView) {
	if trx.PurgeSys == nil {
		return
	}
	oldest := oldestReadView(views)
	trx.PurgeSys.Mu.Lock()
	trx.PurgeSys.View = oldest
	trx.PurgeSys.Mu.Unlock()
}

func oldestReadView(views []*read.ReadView) *read.ReadView {
	var oldest *read.ReadView
	for _, view := range views {
		if view == nil {
			continue
		}
		if oldest == nil ||
			view.LowLimitID < oldest.LowLimitID ||
			(view.LowLimitID == oldest.LowLimitID && view.CreatorTrxID < oldest.CreatorTrxID) {
			oldest = view
		}
	}
	return oldest
}
