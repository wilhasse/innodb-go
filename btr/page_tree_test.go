package btr

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/wilhasse/innodb-go/buf"
	"github.com/wilhasse/innodb-go/fil"
	"github.com/wilhasse/innodb-go/fsp"
	ibos "github.com/wilhasse/innodb-go/os"
	"github.com/wilhasse/innodb-go/page"
)

func setupPageTree(t *testing.T) (*PageTree, func()) {
	t.Helper()
	oldPools := append([]*buf.Pool(nil), buf.DefaultPools()...)
	buf.SetDefaultPools(nil)

	fil.VarInit()
	fsp.Init()
	if !fil.SpaceCreate("ptree", 1, 0, fil.SpaceTablespace) {
		t.Fatalf("SpaceCreate failed")
	}
	path := filepath.Join(t.TempDir(), "ptree.ibd")
	file, err := ibos.FileCreateSimple(path, ibos.FileOverwrite, ibos.FileReadWrite)
	if err != nil {
		t.Fatalf("FileCreateSimple: %v", err)
	}
	if err := fil.SpaceSetFile(1, file); err != nil {
		_ = ibos.FileClose(file)
		t.Fatalf("SpaceSetFile: %v", err)
	}
	cleanup := func() {
		_ = ibos.FileClose(file)
		fil.SpaceDrop(1)
		buf.SetDefaultPools(oldPools)
	}
	return NewPageTree(1, bytes.Compare), cleanup
}

func TestPageTreeInsertSearch(t *testing.T) {
	tree, cleanup := setupPageTree(t)
	defer cleanup()

	replaced, err := tree.Insert([]byte("b"), []byte("vb"))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if replaced {
		t.Fatalf("expected no replace on first insert")
	}
	_, _ = tree.Insert([]byte("a"), []byte("va"))
	_, _ = tree.Insert([]byte("c"), []byte("vc"))

	val, ok, err := tree.Search([]byte("b"))
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if !ok || string(val) != "vb" {
		t.Fatalf("expected vb, got %q ok=%v", val, ok)
	}
	if _, ok, _ := tree.Search([]byte("d")); ok {
		t.Fatalf("expected missing key")
	}

	replaced, err = tree.Insert([]byte("b"), []byte("vb2"))
	if err != nil {
		t.Fatalf("replace: %v", err)
	}
	if !replaced {
		t.Fatalf("expected replace on duplicate insert")
	}
	val, ok, err = tree.Search([]byte("b"))
	if err != nil {
		t.Fatalf("search updated: %v", err)
	}
	if !ok || string(val) != "vb2" {
		t.Fatalf("expected vb2, got %q ok=%v", val, ok)
	}
	if tree.Size() != 3 {
		t.Fatalf("expected size 3, got %d", tree.Size())
	}
}

func TestPageTreeSplit(t *testing.T) {
	tree, cleanup := setupPageTree(t)
	defer cleanup()

	tree.MaxRecs = 3
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		if _, err := tree.Insert([]byte(key), []byte("v"+key)); err != nil {
			t.Fatalf("insert %s: %v", key, err)
		}
	}
	if tree.RootPage == fil.NullPageOffset {
		t.Fatalf("expected root page")
	}
	level, err := tree.pageLevel(tree.RootPage)
	if err != nil {
		t.Fatalf("root level: %v", err)
	}
	if level == 0 {
		t.Fatalf("expected root split")
	}

	left, err := leftmostLeaf(tree)
	if err != nil {
		t.Fatalf("leftmost: %v", err)
	}
	pageBytes, err := fil.SpaceReadPage(tree.SpaceID, left)
	if err != nil {
		t.Fatalf("read leaf: %v", err)
	}
	if page.PageGetNext(pageBytes) == fil.NullPageOffset {
		t.Fatalf("expected leaf next link")
	}

	keys, err := collectLeafKeys(tree)
	if err != nil {
		t.Fatalf("collect keys: %v", err)
	}
	if len(keys) != 5 {
		t.Fatalf("expected 5 keys, got %d", len(keys))
	}
	for i, key := range []string{"a", "b", "c", "d", "e"} {
		if keys[i] != key {
			t.Fatalf("keys[%d]=%s", i, keys[i])
		}
	}
}

func leftmostLeaf(tree *PageTree) (uint32, error) {
	if tree == nil || tree.RootPage == fil.NullPageOffset {
		return 0, errors.New("no root")
	}
	pageNo := tree.RootPage
	for {
		pageBytes, err := fil.SpaceReadPage(tree.SpaceID, pageNo)
		if err != nil {
			return 0, err
		}
		level := page.PageGetLevel(pageBytes)
		if level == 0 {
			return pageNo, nil
		}
		records := collectUserRecords(pageBytes)
		if len(records) == 0 {
			return 0, errors.New("empty internal page")
		}
		_, child, ok := decodeNodePtrRecord(records[0])
		if !ok {
			return 0, errors.New("invalid node pointer")
		}
		pageNo = child
	}
}

func collectLeafKeys(tree *PageTree) ([]string, error) {
	start, err := leftmostLeaf(tree)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0)
	pageNo := start
	for !isNullPageNo(pageNo) {
		pageBytes, err := fil.SpaceReadPage(tree.SpaceID, pageNo)
		if err != nil {
			return nil, err
		}
		records := collectUserRecords(pageBytes)
		for _, recBytes := range records {
			key, ok := recordKey(recBytes)
			if ok {
				keys = append(keys, string(key))
			}
		}
		pageNo = page.PageGetNext(pageBytes)
	}
	return keys, nil
}
