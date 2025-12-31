package page

import (
	"testing"

	"github.com/wilhasse/innodb-go/ut"
)

func TestHeaderFields(t *testing.T) {
	page := make([]byte, ut.UNIV_PAGE_SIZE)

	PageSetPageNo(page, 42)
	if got := PageGetPageNo(page); got != 42 {
		t.Fatalf("page_no=%d", got)
	}

	HeaderSetField(page, PageNRecs, 7)
	if got := HeaderGetField(page, PageNRecs); got != 7 {
		t.Fatalf("n_recs=%d", got)
	}

	PageSetLevel(page, 3)
	if got := PageGetLevel(page); got != 3 {
		t.Fatalf("level=%d", got)
	}

	PageSetNRecs(page, 9)
	if got := PageGetNRecs(page); got != 9 {
		t.Fatalf("n_recs=%d", got)
	}
}
