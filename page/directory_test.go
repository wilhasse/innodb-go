package page

import (
	"testing"

	"github.com/wilhasse/innodb-go/rec"
	"github.com/wilhasse/innodb-go/ut"
)

func TestDirSlotManipulation(t *testing.T) {
	page := make([]byte, ut.UNIV_PAGE_SIZE)
	recOff := uint16(128)

	DirSlotSetRecOffset(page, 0, recOff)
	if got := DirSlotGetRecOffset(page, 0); got != recOff {
		t.Fatalf("rec_off=%d", got)
	}

	rec.HeaderSetNOwned(page[recOff:], 3)
	if got := DirSlotGetNOwned(page, 0); got != 3 {
		t.Fatalf("n_owned=%d", got)
	}

	DirSlotSetNOwned(page, 0, 6)
	if got := rec.HeaderNOwned(page[recOff:]); got != 6 {
		t.Fatalf("n_owned=%d", got)
	}
}
