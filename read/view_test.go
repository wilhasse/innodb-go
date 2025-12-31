package read

import (
	"reflect"
	"testing"
)

func TestReadViewVisibility(t *testing.T) {
	view := NewReadView(10, []uint64{14, 12})
	if !view.Sees(9) {
		t.Fatalf("expected trx 9 visible")
	}
	if !view.Sees(10) {
		t.Fatalf("expected creator visible")
	}
	if view.Sees(12) {
		t.Fatalf("expected trx 12 not visible")
	}
	if view.Sees(14) {
		t.Fatalf("expected trx 14 not visible")
	}
	if !view.Sees(13) {
		t.Fatalf("expected trx 13 visible")
	}
	if view.Sees(view.LowLimitID) {
		t.Fatalf("expected low limit not visible")
	}
}

func TestViewListCopyAndClose(t *testing.T) {
	list := &ViewList{}
	orig := list.Open(10, []uint64{14, 12})
	copy := list.OldestCopyOrOpenNew(20, nil)
	if len(list.Views) != 2 {
		t.Fatalf("views=%d", len(list.Views))
	}
	if copy.CreatorTrxID != 20 {
		t.Fatalf("creator=%d", copy.CreatorTrxID)
	}
	expected := []uint64{14, 12, 10}
	if !reflect.DeepEqual(copy.TrxIDs, expected) {
		t.Fatalf("trx ids=%v", copy.TrxIDs)
	}
	if copy.LowLimitID != orig.LowLimitID {
		t.Fatalf("low limit=%d", copy.LowLimitID)
	}
	if copy.UpLimitID != 10 {
		t.Fatalf("up limit=%d", copy.UpLimitID)
	}
	list.Close(orig)
	if len(list.Views) != 1 || list.Views[0] != copy {
		t.Fatalf("views=%v", list.Views)
	}
}
