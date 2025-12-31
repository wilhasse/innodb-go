package page

import "testing"

func TestCursorSearchInsertDelete(t *testing.T) {
	p := &Page{
		Records: []Record{
			{Key: []byte("a"), Value: []byte("1")},
			{Key: []byte("c"), Value: []byte("3")},
			{Key: []byte("e"), Value: []byte("5")},
		},
	}
	cur := p.NewCursor()

	if found := cur.Search([]byte("c")); !found {
		t.Fatalf("expected to find c")
	}
	if rec := cur.Record(); rec == nil || string(rec.Value) != "3" {
		t.Fatalf("unexpected record at c")
	}

	if found := cur.Search([]byte("b")); found {
		t.Fatalf("did not expect to find b")
	}
	if cur.Index != 1 {
		t.Fatalf("expected insertion index 1, got %d", cur.Index)
	}
	cur.Insert(Record{Key: []byte("b"), Value: []byte("2")})

	if len(p.Records) != 4 || string(p.Records[1].Key) != "b" {
		t.Fatalf("insert failed")
	}

	cur.Delete()
	if len(p.Records) != 3 || string(p.Records[1].Key) != "c" {
		t.Fatalf("delete failed")
	}
}

func TestCursorWalk(t *testing.T) {
	p := &Page{
		Records: []Record{
			{Key: []byte("a")},
			{Key: []byte("b")},
			{Key: []byte("c")},
		},
	}
	cur := p.NewCursor()
	if !cur.First() {
		t.Fatalf("expected First")
	}
	if cur.Record() == nil || string(cur.Record().Key) != "a" {
		t.Fatalf("expected a")
	}
	if !cur.Next() || string(cur.Record().Key) != "b" {
		t.Fatalf("expected b")
	}
	if !cur.Next() || string(cur.Record().Key) != "c" {
		t.Fatalf("expected c")
	}
	if cur.Next() {
		t.Fatalf("did not expect Next at end")
	}
	if !cur.Prev() || string(cur.Record().Key) != "b" {
		t.Fatalf("expected b after Prev")
	}
}
