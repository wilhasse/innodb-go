package trx

import "testing"

func TestTrxAssignReadViewLifecycle(t *testing.T) {
	TrxSysVarInit()
	TrxSysInit()

	trx1 := TrxCreate()
	TrxBegin(trx1)
	trx2 := TrxCreate()
	TrxBegin(trx2)

	view := TrxAssignReadView(trx2)
	if view == nil {
		t.Fatalf("expected read view")
	}
	if view.CreatorTrxID != trx2.ID {
		t.Fatalf("creator=%d want %d", view.CreatorTrxID, trx2.ID)
	}
	if view.Sees(trx1.ID) {
		t.Fatalf("expected trx1 not visible")
	}
	if !view.Sees(trx2.ID) {
		t.Fatalf("expected creator visible")
	}
	if TrxSys == nil || TrxSys.ReadViews == nil {
		t.Fatalf("read views not initialized")
	}
	if len(TrxSys.ReadViews.Views) != 1 {
		t.Fatalf("read views=%d", len(TrxSys.ReadViews.Views))
	}

	view2 := TrxAssignReadView(trx2)
	if view2 != view || len(TrxSys.ReadViews.Views) != 1 {
		t.Fatalf("read view duplicated")
	}

	TrxCommit(trx2)
	if TrxSys.ReadViews != nil && len(TrxSys.ReadViews.Views) != 0 {
		t.Fatalf("expected read views cleared")
	}
	TrxCommit(trx1)
}
