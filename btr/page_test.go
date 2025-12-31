package btr

import (
	"testing"

	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/rem"
)

func TestPageCreateEmptyInsertIterate(t *testing.T) {
	p := PageCreate(1, 1)
	if p == nil {
		t.Fatalf("expected page")
	}
	if len(p.Records) < 2 {
		t.Fatalf("expected infimum/supremum records")
	}
	if p.Records[0].Type != rem.RecordInfimum {
		t.Fatalf("expected infimum at start")
	}
	if p.Records[len(p.Records)-1].Type != rem.RecordSupremum {
		t.Fatalf("expected supremum at end")
	}
	if p.RecordCount() != 0 {
		t.Fatalf("expected no user records, got %d", p.RecordCount())
	}

	p.InsertRecord(page.Record{Key: []byte("b")})
	p.InsertRecord(page.Record{Key: []byte("a")})
	p.InsertRecord(page.Record{Key: []byte("c")})
	if p.RecordCount() != 3 {
		t.Fatalf("expected 3 user records, got %d", p.RecordCount())
	}
	if p.Records[0].Type != rem.RecordInfimum || p.Records[len(p.Records)-1].Type != rem.RecordSupremum {
		t.Fatalf("infimum/supremum not preserved")
	}

	cur := p.NewCursor()
	var got []string
	if cur.First() {
		for {
			rec := cur.Record()
			if rec == nil {
				t.Fatalf("expected record")
			}
			got = append(got, string(rec.Key))
			if !cur.Next() {
				break
			}
		}
	}
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got=%v, want=%v", got, want)
		}
	}
}
