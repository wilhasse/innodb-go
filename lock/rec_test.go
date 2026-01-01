package lock

import "testing"

func TestRecordBitOps(t *testing.T) {
	lock := &Lock{Type: LockRec}
	lock.SetBit(3)
	if !lock.HasBit(3) {
		t.Fatalf("expected bit to be set")
	}
	if len(lock.Bits) < 4 {
		t.Fatalf("expected bitset to grow")
	}
	lock.ClearBit(3)
	if lock.HasBit(3) {
		t.Fatalf("expected bit to be cleared")
	}
}

func TestRecLockIterators(t *testing.T) {
	queue := &Queue{}
	l1 := &Lock{Type: LockRec}
	l1.SetBit(1)
	l2 := &Lock{Type: LockRec}
	l2.SetBit(2)
	l3 := &Lock{Type: LockRec}
	l3.SetBit(1)
	queue.Append(l1)
	queue.Append(l2)
	queue.Append(l3)

	if got := RecLockFirst(queue, 1); got != l1 {
		t.Fatalf("expected first lock for heap 1")
	}
	if got := RecLockNext(l1, 1); got != l3 {
		t.Fatalf("expected next lock for heap 1")
	}
	if got := RecLockNext(l3, 1); got != nil {
		t.Fatalf("expected no further lock for heap 1")
	}
	if got := RecLockFirst(queue, 2); got != l2 {
		t.Fatalf("expected first lock for heap 2")
	}
	if got := RecLockFirst(queue, 3); got != nil {
		t.Fatalf("expected no lock for heap 3")
	}
}
