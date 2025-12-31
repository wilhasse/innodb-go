package fsp

import (
	"testing"

	"github.com/wilhasse/innodb-go/fil"
)

func TestHeaderInitFields(t *testing.T) {
	page := make([]byte, HeaderOffset+SpaceFlagsOffset+4)
	HeaderInitFields(page, 7, 99)

	if got := HeaderGetSpaceID(page); got != 7 {
		t.Fatalf("expected space id 7, got %d", got)
	}
	if got := HeaderGetFlags(page); got != 99 {
		t.Fatalf("expected flags 99, got %d", got)
	}
	if got := HeaderGetZipSize(page); got != 99 {
		t.Fatalf("expected zip size 99, got %d", got)
	}
}

func TestHeaderInitAndSize(t *testing.T) {
	page := make([]byte, HeaderOffset+SpaceFlagsOffset+4)
	HeaderInit(page, 5, 100, 0)

	if got := HeaderGetSpaceID(page); got != 5 {
		t.Fatalf("expected space id 5, got %d", got)
	}
	if got := GetSizeLow(page); got != 100 {
		t.Fatalf("expected size 100, got %d", got)
	}
	if got := HeaderGetFreeLimit(); got != 100 {
		t.Fatalf("expected free limit 100, got %d", got)
	}
}

func TestHeaderIncSize(t *testing.T) {
	fil.VarInit()
	if !fil.SpaceCreate("sys", 0, 0, fil.SpaceTablespace) {
		t.Fatalf("expected space create to succeed")
	}
	HeaderIncSize(0, 10)
	HeaderIncSize(0, 5)
	if got := fil.SpaceGetSize(0); got != 15 {
		t.Fatalf("expected size 15, got %d", got)
	}
	if got := HeaderGetTablespaceSize(); got != 15 {
		t.Fatalf("expected tablespace size 15, got %d", got)
	}
}
