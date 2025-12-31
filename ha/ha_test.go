package ha

import "testing"

func TestHashTableInsertSearchUpdate(t *testing.T) {
	table := Create(4)
	if table == nil {
		t.Fatalf("expected table")
	}
	if !InsertForFold(table, 10, "a") {
		t.Fatalf("expected insert to succeed")
	}
	if got := SearchAndGetData(table, 10); got != "a" {
		t.Fatalf("expected to find a, got %v", got)
	}
	if !InsertForFold(table, 10, "b") {
		t.Fatalf("expected update to succeed")
	}
	if got := SearchAndGetData(table, 10); got != "b" {
		t.Fatalf("expected to find b, got %v", got)
	}
}

func TestHashTableSearchUpdateDelete(t *testing.T) {
	table := Create(3)
	InsertForFold(table, 7, "alpha")
	InsertForFold(table, 11, "beta")

	SearchAndUpdateIfFound(table, 7, "alpha", "gamma")
	if got := SearchAndGetData(table, 7); got != "gamma" {
		t.Fatalf("expected gamma, got %v", got)
	}
	SearchAndUpdateIfFound(table, 7, "alpha", "delta")
	if got := SearchAndGetData(table, 7); got != "gamma" {
		t.Fatalf("expected gamma to remain, got %v", got)
	}

	if !SearchAndDeleteIfFound(table, 11, "beta") {
		t.Fatalf("expected delete to succeed")
	}
	if got := SearchAndGetData(table, 11); got != nil {
		t.Fatalf("expected beta to be removed")
	}
}

func TestHashTableClear(t *testing.T) {
	table := Create(2)
	InsertForFold(table, 1, "one")
	InsertForFold(table, 2, "two")
	table.Clear()
	if got := SearchAndGetData(table, 1); got != nil {
		t.Fatalf("expected table to be cleared")
	}
	if got := SearchAndGetData(table, 2); got != nil {
		t.Fatalf("expected table to be cleared")
	}
}
