package btr

import (
	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/rem"
)

// PageCreate allocates and initializes a B-tree index page.
func PageCreate(spaceID, pageNo uint32) *page.Page {
	p := page.NewPage(spaceID, pageNo, page.PageTypeIndex)
	PageEmpty(p)
	return p
}

// PageEmpty resets the page and installs infimum/supremum records.
func PageEmpty(p *page.Page) {
	if p == nil {
		return
	}
	p.Records = nil
	p.NextHeapNo = rem.HeapNoSupremum + 1
	p.Records = append(p.Records, page.Record{
		Type:   rem.RecordInfimum,
		HeapNo: rem.HeapNoInfimum,
		Key:    []byte("infimum"),
	})
	p.Records = append(p.Records, page.Record{
		Type:   rem.RecordSupremum,
		HeapNo: rem.HeapNoSupremum,
		Key:    []byte("supremum"),
	})
}
