package ha

import "testing"

func TestHashCreatePrime(t *testing.T) {
	table := HashCreate(10)
	if table == nil {
		t.Fatalf("expected table")
	}
	if got := len(table.buckets); got < 10 {
		t.Fatalf("expected at least 10 buckets, got %d", got)
	}
	if !isPrime(len(table.buckets)) {
		t.Fatalf("expected prime bucket count, got %d", len(table.buckets))
	}
}

func TestHashMutexes(t *testing.T) {
	table := HashCreate(3)
	HashCreateMutexes(table, 4)
	if len(table.mutexes) != 4 {
		t.Fatalf("expected 4 mutexes, got %d", len(table.mutexes))
	}
	HashMutexEnter(table, 7)
	HashMutexExit(table, 7)
	HashMutexEnterAll(table)
	HashMutexExitAll(table)
	HashFreeMutexes(table)
	if len(table.mutexes) != 0 {
		t.Fatalf("expected mutexes to be freed")
	}
}
