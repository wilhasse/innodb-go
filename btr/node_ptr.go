package btr

import (
	"github.com/wilhasse/innodb-go/mach"
	"github.com/wilhasse/innodb-go/page"
	"github.com/wilhasse/innodb-go/rem"
)

const nodePtrValueLen = 4

// NodePtrSetChildPageNo stores the child page number in a node pointer record.
func NodePtrSetChildPageNo(parent *page.Page, rec *page.Record, pageNo uint32) {
	if rec == nil {
		return
	}
	if rec.Type == rem.RecordInfimum || rec.Type == rem.RecordSupremum {
		return
	}
	if len(rec.Value) < nodePtrValueLen {
		rec.Value = make([]byte, nodePtrValueLen)
	} else {
		rec.Value = rec.Value[:nodePtrValueLen]
	}
	mach.WriteTo4(rec.Value, pageNo)
	rec.Type = rem.RecordNodePointer

	if parent == nil {
		return
	}
	child := page.GetPage(parent.SpaceID, pageNo)
	if child != nil {
		child.ParentPageNo = parent.PageNo
	}
}

// NodePtrGetChild returns the child page number from a node pointer record.
func NodePtrGetChild(rec *page.Record) uint32 {
	if rec == nil {
		return 0
	}
	return mach.ReadFrom4(rec.Value)
}

// PageGetFatherNodePtr returns the node pointer record in the parent page.
func PageGetFatherNodePtr(child *page.Page) *page.Record {
	parent := PageGetFatherBlock(child)
	if parent == nil || child == nil {
		return nil
	}
	return findNodePtrForChild(parent, child.PageNo)
}

// PageGetFatherBlock returns the parent page for the given child.
func PageGetFatherBlock(child *page.Page) *page.Page {
	if child == nil || child.ParentPageNo == 0 {
		return nil
	}
	return page.GetPage(child.SpaceID, child.ParentPageNo)
}

// PageGetFather returns the parent page and node pointer record.
func PageGetFather(child *page.Page) (*page.Page, *page.Record) {
	parent := PageGetFatherBlock(child)
	if parent == nil || child == nil {
		return nil, nil
	}
	return parent, findNodePtrForChild(parent, child.PageNo)
}

func findNodePtrForChild(parent *page.Page, childPageNo uint32) *page.Record {
	if parent == nil {
		return nil
	}
	for i := range parent.Records {
		rec := &parent.Records[i]
		if rec.Type != rem.RecordNodePointer {
			continue
		}
		if NodePtrGetChild(rec) == childPageNo {
			return rec
		}
	}
	return nil
}
