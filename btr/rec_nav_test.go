package btr

import (
	"testing"

	"github.com/wilhasse/innodb-go/page"
)

func TestGetNextPrevUserRecAcrossPages(t *testing.T) {
	oldRegistry := page.PageRegistry
	page.PageRegistry = page.NewRegistry()
	defer func() {
		page.PageRegistry = oldRegistry
	}()

	p1 := PageCreate(1, 1)
	p2 := PageCreate(1, 2)
	p3 := PageCreate(1, 3)
	p1.NextPage = 2
	p2.PrevPage = 1
	p2.NextPage = 3
	p3.PrevPage = 2

	page.RegisterPage(p1)
	page.RegisterPage(p2)
	page.RegisterPage(p3)

	p1.InsertRecord(page.Record{Key: []byte("a")})
	p1.InsertRecord(page.Record{Key: []byte("b")})
	p2.InsertRecord(page.Record{Key: []byte("c")})
	p2.InsertRecord(page.Record{Key: []byte("d")})
	p3.InsertRecord(page.Record{Key: []byte("e")})

	cur := p1.NewCursor()
	if !cur.First() {
		t.Fatalf("expected first record")
	}
	var forward []string
	for {
		rec := cur.Record()
		if rec == nil {
			t.Fatalf("expected record during forward scan")
		}
		forward = append(forward, string(rec.Key))
		if GetNextUserRec(cur) == nil {
			break
		}
	}
	wantForward := []string{"a", "b", "c", "d", "e"}
	if len(forward) != len(wantForward) {
		t.Fatalf("forward count mismatch: got %v want %v", forward, wantForward)
	}
	for i := range wantForward {
		if forward[i] != wantForward[i] {
			t.Fatalf("forward mismatch: got %v want %v", forward, wantForward)
		}
	}

	cur = p3.NewCursor()
	if !cur.Last() {
		t.Fatalf("expected last record")
	}
	var backward []string
	for {
		rec := cur.Record()
		if rec == nil {
			t.Fatalf("expected record during backward scan")
		}
		backward = append(backward, string(rec.Key))
		if GetPrevUserRec(cur) == nil {
			break
		}
	}
	wantBackward := []string{"e", "d", "c", "b", "a"}
	if len(backward) != len(wantBackward) {
		t.Fatalf("backward count mismatch: got %v want %v", backward, wantBackward)
	}
	for i := range wantBackward {
		if backward[i] != wantBackward[i] {
			t.Fatalf("backward mismatch: got %v want %v", backward, wantBackward)
		}
	}
}
