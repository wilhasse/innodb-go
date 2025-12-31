package fil

import "testing"

func TestSpaceCreateAndNodeSize(t *testing.T) {
	VarInit()
	if !SpaceCreate("ts1", 1, 0, SpaceTablespace) {
		t.Fatalf("expected space create to succeed")
	}
	if _, err := NodeCreate("ts1.ibd", 10, 1, false); err != nil {
		t.Fatalf("expected node create to succeed: %v", err)
	}
	if _, err := NodeCreate("ts1_part2.ibd", 5, 1, false); err != nil {
		t.Fatalf("expected second node create to succeed: %v", err)
	}
	if got := SpaceGetSize(1); got != 15 {
		t.Fatalf("expected size 15, got %d", got)
	}
	if SpaceGetType(1) != SpaceTablespace {
		t.Fatalf("expected tablespace type")
	}
}

func TestSpaceCreateDuplicate(t *testing.T) {
	VarInit()
	if !SpaceCreate("ts1", 1, 0, SpaceTablespace) {
		t.Fatalf("expected space create to succeed")
	}
	if SpaceCreate("ts1", 2, 0, SpaceTablespace) {
		t.Fatalf("expected duplicate name to fail")
	}
	if SpaceCreate("ts2", 1, 0, SpaceTablespace) {
		t.Fatalf("expected duplicate id to fail")
	}
}

func TestSpaceVersionAndDeletion(t *testing.T) {
	VarInit()
	if got := SpaceGetVersion(1); got != -1 {
		t.Fatalf("expected missing version to be -1, got %d", got)
	}
	if !TablespaceDeletedOrBeingDeletedInMem(1, -1) {
		t.Fatalf("expected missing tablespace to report deleted")
	}

	if !SpaceCreate("ts1", 1, 0, SpaceTablespace) {
		t.Fatalf("expected space create to succeed")
	}
	space := SpaceGetByID(1)
	if space == nil {
		t.Fatalf("expected space lookup to succeed")
	}
	space.Version = 7
	if got := SpaceGetVersion(1); got != 7 {
		t.Fatalf("expected version 7, got %d", got)
	}
	if TablespaceDeletedOrBeingDeletedInMem(1, 7) {
		t.Fatalf("expected version match to report existing")
	}
	if !TablespaceDeletedOrBeingDeletedInMem(1, 8) {
		t.Fatalf("expected version mismatch to report deleted")
	}
	space.IsBeingDeleted = true
	if !TablespaceDeletedOrBeingDeletedInMem(1, -1) {
		t.Fatalf("expected deleted flag to report deleted")
	}
	if TablespaceExistsInMem(1) {
		t.Fatalf("expected deleted space to be missing")
	}
}
