package buf

import "testing"

func TestBuddyAllocFree(t *testing.T) {
	alloc, err := NewBuddyAllocator(BufBuddyHigh)
	if err != nil {
		t.Fatalf("unexpected allocator error: %v", err)
	}

	block, ok := alloc.Alloc(BufBuddyLow)
	if !ok || block == nil {
		t.Fatalf("expected allocation to succeed")
	}
	if block.Size() != BufBuddyLow {
		t.Fatalf("expected block size %d, got %d", BufBuddyLow, block.Size())
	}
	if len(BufBuddyStat) <= block.level || BufBuddyStat[block.level].Used != 1 {
		t.Fatalf("expected stats to record allocation")
	}
	if err := alloc.Free(block); err != nil {
		t.Fatalf("unexpected free error: %v", err)
	}
	if BufBuddyStat[block.level].Used != 0 {
		t.Fatalf("expected stats to record free")
	}
}

func TestBuddySplitMerge(t *testing.T) {
	alloc, err := NewBuddyAllocator(BufBuddyHigh)
	if err != nil {
		t.Fatalf("unexpected allocator error: %v", err)
	}

	block1, ok := alloc.Alloc(BufBuddyLow)
	if !ok {
		t.Fatalf("expected allocation to succeed")
	}
	block2, ok := alloc.Alloc(BufBuddyLow)
	if !ok {
		t.Fatalf("expected second allocation to succeed")
	}

	if err := alloc.Free(block1); err != nil {
		t.Fatalf("unexpected free error: %v", err)
	}
	if err := alloc.Free(block2); err != nil {
		t.Fatalf("unexpected free error: %v", err)
	}

	blockBig, ok := alloc.Alloc(BufBuddyHigh)
	if !ok || blockBig == nil {
		t.Fatalf("expected full allocation after merge")
	}
}

func TestBuddyAllocRounding(t *testing.T) {
	alloc, err := NewBuddyAllocator(BufBuddyHigh)
	if err != nil {
		t.Fatalf("unexpected allocator error: %v", err)
	}

	block, ok := alloc.Alloc(BufBuddyLow + 1)
	if !ok {
		t.Fatalf("expected allocation to succeed")
	}
	if block.Size() != BufBuddyLow*2 {
		t.Fatalf("expected rounded size %d, got %d", BufBuddyLow*2, block.Size())
	}
}

func TestBuddyAllocExhaustion(t *testing.T) {
	alloc, err := NewBuddyAllocator(BufBuddyHigh)
	if err != nil {
		t.Fatalf("unexpected allocator error: %v", err)
	}

	block, ok := alloc.Alloc(BufBuddyHigh)
	if !ok {
		t.Fatalf("expected allocation to succeed")
	}
	if _, ok := alloc.Alloc(BufBuddyLow); ok {
		t.Fatalf("expected allocator to be exhausted")
	}
	if err := alloc.Free(block); err != nil {
		t.Fatalf("unexpected free error: %v", err)
	}
}
