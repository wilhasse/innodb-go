package btr

import (
	"testing"

	"github.com/wilhasse/innodb-go/dict"
	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/fsp"
	"github.com/wilhasse/innodb-go/page"
)

func TestTreeCreateRootManagement(t *testing.T) {
	oldRegistry := page.PageRegistry
	page.PageRegistry = page.NewRegistry()
	defer func() {
		page.PageRegistry = oldRegistry
	}()

	fil.VarInit()
	fsp.Init()
	if !fil.SpaceCreate("ts2", 2, 0, fil.SpaceTablespace) {
		t.Fatalf("expected space create")
	}

	idx := &dict.Index{Name: "idx_root", SpaceID: 2}
	root := Create(idx)
	if root == nil {
		t.Fatalf("expected root page")
	}
	if idx.RootPage != root.PageNo {
		t.Fatalf("root page mismatch: got %d want %d", idx.RootPage, root.PageNo)
	}
	if idx.TreeLevel != 0 {
		t.Fatalf("expected tree level 0, got %d", idx.TreeLevel)
	}
	if got := RootBlockGet(idx); got != root {
		t.Fatalf("root block mismatch")
	}
	if got := RootGet(idx); got != root {
		t.Fatalf("root get mismatch")
	}
	if got := GetSize(idx); got != 1 {
		t.Fatalf("size mismatch: got %d want 1", got)
	}

	extra := PageAlloc(idx)
	if extra == nil {
		t.Fatalf("expected extra page")
	}
	if got := GetSize(idx); got != 2 {
		t.Fatalf("size after alloc mismatch: got %d want 2", got)
	}

	FreeButNotRoot(idx)
	if RootGet(idx) == nil {
		t.Fatalf("expected root to remain")
	}
	if got := GetSize(idx); got != 1 {
		t.Fatalf("size after free but not root mismatch: got %d want 1", got)
	}
	if page.GetPage(idx.SpaceID, extra.PageNo) != nil {
		t.Fatalf("expected extra page freed")
	}

	FreeRoot(idx)
	if idx.RootPage != 0 || idx.TreeLevel != 0 {
		t.Fatalf("expected root metadata cleared")
	}
	if RootGet(idx) != nil {
		t.Fatalf("expected root removed")
	}
}
