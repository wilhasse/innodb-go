package page

import "testing"

func TestPageInit(t *testing.T) {
	p := &Page{Records: []Record{{Key: []byte("x")}}}
	p.Init(1, 2, PageTypeIndex)
	if p.SpaceID != 1 || p.PageNo != 2 || p.PageType != PageTypeIndex {
		t.Fatalf("header not set")
	}
	if !p.IsEmpty() {
		t.Fatalf("expected empty page")
	}
}

func TestPageInsertDelete(t *testing.T) {
	p := NewPage(0, 0, PageTypeIndex)
	p.InsertRecord(Record{Key: []byte("b")})
	p.InsertRecord(Record{Key: []byte("a")})
	p.InsertRecord(Record{Key: []byte("c")})
	if p.RecordCount() != 3 {
		t.Fatalf("count=%d", p.RecordCount())
	}
	if string(p.Records[0].Key) != "a" || string(p.Records[1].Key) != "b" {
		t.Fatalf("records not sorted")
	}
	if rec := p.FindRecord([]byte("b")); rec == nil {
		t.Fatalf("expected record b")
	}
	if !p.DeleteRecord([]byte("b")) {
		t.Fatalf("delete failed")
	}
	if p.FindRecord([]byte("b")) != nil {
		t.Fatalf("expected record removed")
	}
}
