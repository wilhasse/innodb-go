package trx

import "testing"

func TestTrxSysInitAndClose(t *testing.T) {
	TrxSysVarInit()
	if TrxSys != nil {
		t.Fatalf("expected nil trx sys")
	}

	TrxSysInit()
	if TrxSys == nil || !TrxSys.Initialized {
		t.Fatalf("expected trx sys initialized")
	}

	id1 := TrxSysAllocID()
	id2 := TrxSysAllocID()
	if id1 != 1 || id2 != 2 {
		t.Fatalf("ids=%d/%d", id1, id2)
	}

	trx := &Trx{}
	TrxSysAddActive(trx)
	if len(TrxSys.Active) != 1 {
		t.Fatalf("active=%d", len(TrxSys.Active))
	}
	TrxSysRemoveActive(trx)
	if len(TrxSys.Active) != 0 {
		t.Fatalf("active=%d", len(TrxSys.Active))
	}

	TrxDoublewriteInit(10, 20, 5)
	if !TrxDoublewritePageInside(12) {
		t.Fatalf("expected page inside block1")
	}
	if !TrxDoublewritePageInside(22) {
		t.Fatalf("expected page inside block2")
	}
	if TrxDoublewritePageInside(30) {
		t.Fatalf("expected page outside")
	}

	TrxSysClose()
	if TrxSys != nil || TrxDoublewrite != nil {
		t.Fatalf("expected trx sys closed")
	}
	if TrxDoublewritePageInside(12) {
		t.Fatalf("expected page outside after close")
	}
}
