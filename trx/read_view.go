package trx

import "github.com/wilhasse/innodb-go/read"

// TrxAssignReadView returns the transaction read view, creating it if needed.
func TrxAssignReadView(trx *Trx) *read.ReadView {
	if trx == nil || trx.State != TrxActive || trx.ID == 0 {
		return nil
	}
	if TrxSys == nil {
		TrxSysInit()
	}
	TrxSys.Mu.Lock()
	defer TrxSys.Mu.Unlock()
	if trx.ReadView != nil {
		return trx.ReadView
	}
	active := make([]uint64, 0, len(TrxSys.Active))
	for _, activeTrx := range TrxSys.Active {
		if activeTrx == nil || activeTrx.ID == 0 || activeTrx == trx {
			continue
		}
		active = append(active, activeTrx.ID)
	}
	if TrxSys.ReadViews == nil {
		TrxSys.ReadViews = &read.ViewList{}
	}
	view := TrxSys.ReadViews.Open(trx.ID, active)
	trx.ReadView = view
	return view
}

// TrxCloseReadView closes and clears the transaction read view.
func TrxCloseReadView(trx *Trx) {
	if trx == nil {
		return
	}
	if TrxSys == nil {
		trx.ReadView = nil
		return
	}
	TrxSys.Mu.Lock()
	if TrxSys.ReadViews != nil && trx.ReadView != nil {
		TrxSys.ReadViews.Close(trx.ReadView)
	}
	trx.ReadView = nil
	TrxSys.Mu.Unlock()
}
