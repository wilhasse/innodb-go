package btr

import (
	"testing"

	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/rem"
)

func TestNodePtrParentNavigation(t *testing.T) {
	oldRegistry := page.PageRegistry
	page.PageRegistry = page.NewRegistry()
	defer func() {
		page.PageRegistry = oldRegistry
	}()

	parent := PageCreate(1, 10)
	child := PageCreate(1, 11)
	page.RegisterPage(parent)
	page.RegisterPage(child)

	rec := page.Record{Type: rem.RecordNodePointer, Key: []byte("k")}
	NodePtrSetChildPageNo(parent, &rec, child.PageNo)
	parent.InsertRecord(rec)

	if got := NodePtrGetChild(&rec); got != child.PageNo {
		t.Fatalf("child page no mismatch: got %d want %d", got, child.PageNo)
	}
	if child.ParentPageNo != parent.PageNo {
		t.Fatalf("child parent mismatch: got %d want %d", child.ParentPageNo, parent.PageNo)
	}
	if got := PageGetFatherBlock(child); got != parent {
		t.Fatalf("father block mismatch: got %v want %v", got, parent)
	}
	fatherPtr := PageGetFatherNodePtr(child)
	if fatherPtr == nil {
		t.Fatalf("expected father node ptr")
	}
	if NodePtrGetChild(fatherPtr) != child.PageNo {
		t.Fatalf("father node ptr child mismatch: got %d want %d", NodePtrGetChild(fatherPtr), child.PageNo)
	}
	fatherPage, fatherRec := PageGetFather(child)
	if fatherPage != parent || fatherRec == nil {
		t.Fatalf("expected father page and rec")
	}
	if NodePtrGetChild(fatherRec) != child.PageNo {
		t.Fatalf("father rec child mismatch: got %d want %d", NodePtrGetChild(fatherRec), child.PageNo)
	}
}
