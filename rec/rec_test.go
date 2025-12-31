package rec

import "testing"

func TestRecConstants(t *testing.T) {
	if RecInfoMinRecFlag != 0x10 {
		t.Fatalf("RecInfoMinRecFlag=%d", RecInfoMinRecFlag)
	}
	if RecInfoDeletedFlag != 0x20 {
		t.Fatalf("RecInfoDeletedFlag=%d", RecInfoDeletedFlag)
	}
	if RecNOldExtraBytes != 6 {
		t.Fatalf("RecNOldExtraBytes=%d", RecNOldExtraBytes)
	}
	if RecNNewExtraBytes != 5 {
		t.Fatalf("RecNNewExtraBytes=%d", RecNNewExtraBytes)
	}
	if RecStatusOrdinary != 0 || RecStatusNodePtr != 1 || RecStatusInfimum != 2 || RecStatusSupremum != 3 {
		t.Fatalf("rec status constants unexpected: %d %d %d %d", RecStatusOrdinary, RecStatusNodePtr, RecStatusInfimum, RecStatusSupremum)
	}
	if RecNewHeapNo != 4 {
		t.Fatalf("RecNewHeapNo=%d", RecNewHeapNo)
	}
	if RecHeapNoShift != 3 {
		t.Fatalf("RecHeapNoShift=%d", RecHeapNoShift)
	}
	if RecNodePtrSize != 4 {
		t.Fatalf("RecNodePtrSize=%d", RecNodePtrSize)
	}
	if RecOffsHeaderSize != 2 {
		t.Fatalf("RecOffsHeaderSize=%d", RecOffsHeaderSize)
	}
	if RecOffsNormalSize != 100 {
		t.Fatalf("RecOffsNormalSize=%d", RecOffsNormalSize)
	}
	if RecOffsSmallSize != 10 {
		t.Fatalf("RecOffsSmallSize=%d", RecOffsSmallSize)
	}
	if RecOffsCompact != uint32(1<<31) {
		t.Fatalf("RecOffsCompact=%d", RecOffsCompact)
	}
	if RecOffsSQLNull != uint32(1<<31) {
		t.Fatalf("RecOffsSQLNull=%d", RecOffsSQLNull)
	}
	if RecOffsExternal != uint32(1<<30) {
		t.Fatalf("RecOffsExternal=%d", RecOffsExternal)
	}
	if RecOffsMask != RecOffsExternal-1 {
		t.Fatalf("RecOffsMask=%d", RecOffsMask)
	}
}
