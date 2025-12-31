package thr

import (
	"testing"

	"github.com/wilhasse/innodb-go/os"
	"github.com/wilhasse/innodb-go/ut"
)

func TestLocalSlotIsolation(t *testing.T) {
	LocalClose()
	LocalInit()

	id := os.ThreadGetCurrID()
	LocalSetSlotNo(id, ut.Ulint(7))
	if got := LocalGetSlotNo(id); got != ut.Ulint(7) {
		t.Fatalf("slot=%d", got)
	}

	ch := make(chan ut.Ulint, 1)
	go func() {
		otherID := os.ThreadGetCurrID()
		LocalSetSlotNo(otherID, ut.Ulint(42))
		ch <- LocalGetSlotNo(otherID)
	}()

	if got := <-ch; got != ut.Ulint(42) {
		t.Fatalf("other slot=%d", got)
	}
	if got := LocalGetSlotNo(id); got != ut.Ulint(7) {
		t.Fatalf("slot=%d", got)
	}

	LocalClose()
}

func TestLocalInIbufField(t *testing.T) {
	LocalClose()
	LocalInit()

	field := LocalGetInIbufField()
	if field == nil {
		t.Fatalf("expected in-ibuf field")
	}
	*field = ut.IBool(1)
	if got := *LocalGetInIbufField(); got != ut.IBool(1) {
		t.Fatalf("in-ibuf=%d", got)
	}

	ch := make(chan ut.IBool, 1)
	go func() {
		ch <- *LocalGetInIbufField()
	}()
	if got := <-ch; got != ut.IBool(0) {
		t.Fatalf("other in-ibuf=%d", got)
	}

	LocalClose()
}
