package api

import (
	"github.com/wilhasse/innodb-go/trx"
	"github.com/wilhasse/innodb-go/ut"
)

func purgeIfNeeded() {
	if trx.TrxSys == nil {
		return
	}
	trx.TrxSys.Mu.Lock()
	viewCount := 0
	if trx.TrxSys.ReadViews != nil {
		viewCount = len(trx.TrxSys.ReadViews.Views)
	}
	trx.TrxSys.Mu.Unlock()
	if viewCount > 0 {
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
